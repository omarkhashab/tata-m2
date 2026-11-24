"""The warehouse model: table specification (DDL, keys) and the builder of every table.

``TABLES`` is the single source of truth for the PostgreSQL star schema: column order and
types, the declared natural key (the primary key, which the idempotent upsert conflicts
on), foreign keys and indexes. ``ddl()`` renders it (``sql/ddl.sql`` is that rendering,
committed for reading), and the loader uses the same spec to COPY and upsert.

``build_star`` turns the curated M1 output into the dimensional model:

* dimensions  dim_tenant, dim_agent (one row per agent version), dim_tool, dim_policy
  (one row per policy version, flattened in DuckDB), dim_date, dim_time, plus
  dim_model_price and dim_source_file (the catalogue)
* facts       fact_session, fact_prompt_turn, fact_tool_call, fact_network_flow,
  fact_sandbox_decision, fact_siem_alert, fact_rejected_record
* bridges     policy_allowed_tool, policy_egress_rule (UNNEST-ed in DuckDB)

Every fact row keeps its stable source identifier (``event_id``, ``flow_id``, ``alert_id``,
``audit_id``), the canonical ``activity_id`` and a pointer to the exact raw bytes it came
from (``source_file_id`` = file@sha256, plus ``source_line``). Categorical columns stay
as their source values: encodings are for preprocessing only, never stored here.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from kestrel_m1.activity import START, TERMINAL
from kestrel_m1.metrics import session_outcomes
from kestrel_m1.policy import ESCALATION_ACTIONS

from . import config
from .catalog import id_by_file

HOUR_BANDS = [(0, 6, "00-06 night"), (6, 12, "06-12 morning"), (12, 18, "12-18 afternoon"),
              (18, 24, "18-24 evening")]


# =========================================================================== spec

@dataclass
class Col:
    name: str
    type: str
    not_null: bool = False
    check: str | None = None


@dataclass
class Table:
    name: str
    cols: list[Col]
    pk: list[str]
    fks: list[tuple[list[str], str, list[str]]] = field(default_factory=list)
    indexes: list[list[str]] = field(default_factory=list)
    comment: str = ""
    upsert: str = "update"          # "update" = upsert + prune; "nothing" = insert-once (catalogue)
    prune_where: str | None = None  # restrict the prune to this table partition (e.g. batch rejects only)

    @property
    def columns(self) -> list[str]:
        return [c.name for c in self.cols]


def C(name, typ, nn=False, check=None) -> Col:
    return Col(name, typ, nn, check)


SRC = [C("source_file_id", "text", True), C("source_line", "bigint", True)]
REJECT_REASONS = ("DUPLICATE_EVENT_ID", "UNKNOWN_AGENT_ID", "UNKNOWN_TENANT_ID", "MALFORMED_TIMESTAMP", "ORPHAN_EVENT",
                  "INVALID_EVENT_TYPE", "MISSING_REQUIRED_FIELD", "INVALID_NUMERIC_RANGE", "LATE_BEYOND_WATERMARK",
                  "UNPARSEABLE_RECORD", "SCHEMA_VIOLATION")
_reason_check = "reason IN (" + ", ".join(f"'{r}'" for r in REJECT_REASONS) + ")"
AGENT_FK = (["agent_id", "release_version"], "dim_agent", ["agent_id", "release_version"])
TENANT_FK = (["tenant_id"], "dim_tenant", ["tenant_id"])
POLICY_FK = (["sandbox_profile_id", "policy_version_applied"], "dim_policy", ["sandbox_profile_id", "policy_version"])
DATE_FK = (["date_key"], "dim_date", ["date_key"])
TIME_FK = (["time_key"], "dim_time", ["time_key"])
SRC_FK = (["source_file_id"], "dim_source_file", ["source_file_id"])
SESSION_FK = (["session_id"], "fact_session", ["session_id"])
WHEN = [C("date_key", "integer", True), C("time_key", "smallint", True)]
WHO = [C("session_id", "text", True), C("tenant_id", "text", True), C("agent_id", "text", True),
       C("release_version", "text", True), C("agent_sk", "integer", True), C("sandbox_profile_id", "text"),
       C("policy_version_applied", "integer")]
FACT_FKS = [SESSION_FK, TENANT_FK, AGENT_FK, POLICY_FK, DATE_FK, TIME_FK, SRC_FK]

TABLES: list[Table] = [
    Table("dim_source_file", [
        C("source_file_id", "text", True), C("file_name", "text", True), C("path", "text", True),
        C("format", "text", True), C("format_detail", "text", True), C("grain", "text", True),
        C("size_bytes", "bigint", True), C("content_sha256", "char(64)", True), C("row_count", "bigint", True),
        C("used_in", "text", True), C("ingested_at", "timestamptz", True)],
        pk=["source_file_id"], upsert="nothing",
        comment="Lab 6 source-file catalogue: one row per file content (file@sha256)"),
    Table("dim_date", [
        C("date_key", "integer", True), C("full_date", "date", True), C("year", "smallint", True),
        C("quarter", "smallint", True), C("month", "smallint", True), C("month_name", "text", True),
        C("day", "smallint", True), C("day_of_week", "smallint", True), C("day_name", "text", True),
        C("iso_year", "smallint", True), C("iso_week", "smallint", True), C("is_weekend", "boolean", True)],
        pk=["date_key"], comment="calendar built from the data's own timestamps (UTC days)"),
    Table("dim_time", [
        C("time_key", "smallint", True), C("hour", "smallint", True), C("minute", "smallint", True),
        C("hhmm", "char(5)", True), C("hour_band", "text", True), C("is_business_hours", "boolean", True)],
        pk=["time_key"], comment="minute of the UTC day (0..1439) with 6-hour bands"),
    Table("dim_tenant", [
        C("tenant_id", "text", True), C("tenant_name", "text", True), C("industry", "text", True),
        C("plan_tier", "text", True), C("region", "text"), C("region_raw", "text"),
        C("onboarded_at_utc", "timestamptz", True), C("is_active", "boolean", True), C("security_contact", "text"),
        C("merged_tenant_ids", "text[]", True), C("quality_flags", "text", True), *SRC],
        pk=["tenant_id"], fks=[SRC_FK], comment="canonical tenants (tn_004 merged into tn_003)"),
    Table("dim_agent", [
        C("agent_id", "text", True), C("release_version", "text", True), C("agent_sk", "integer", True),
        C("release_version_raw", "text", True), C("agent_name", "text", True), C("agent_type", "text", True),
        C("model_id", "text", True), C("tenant_id", "text", True), C("sandbox_profile_id", "text", True),
        C("owner_team", "text", True), C("autonomy_level", "text", True),
        C("max_tool_calls_per_session", "integer", True), C("launch_date", "date", True),
        C("is_deprecated", "boolean", True), C("valid_from_utc", "timestamptz", True), C("valid_to_utc", "timestamptz"),
        C("is_current", "boolean", True), C("quality_flags", "text", True), *SRC],
        pk=["agent_id", "release_version"], fks=[TENANT_FK, SRC_FK], indexes=[["agent_sk"]],
        comment="one row per agent version (Type-2 window kept as columns)"),
    Table("dim_tool", [
        C("tool_id", "text", True), C("tool_version", "text", True), C("tool_name", "text", True),
        C("tool_category", "text", True), C("risk_tier", "text", True), C("requires_approval", "boolean", True),
        C("owner_team", "text", True), C("quality_flags", "text", True), *SRC],
        pk=["tool_id", "tool_version"], fks=[SRC_FK]),
    Table("dim_policy", [
        C("sandbox_profile_id", "text", True), C("policy_version", "integer", True),
        C("valid_from_utc", "timestamptz", True), C("valid_to_utc", "timestamptz"),
        C("valid_to_declared_utc", "timestamptz"), C("allowed_tools", "text[]", True),
        C("has_network_block", "boolean", True), C("egress_allowlist", "text[]", True),
        C("denied_ports", "integer[]", True), C("max_egress_mb", "double precision"),
        C("fs_read_scope", "text[]", True), C("fs_write_scope", "text[]", True), C("max_runtime_s", "integer", True),
        C("max_memory_mb", "integer", True), C("escalation_allowed", "boolean", True),
        C("quality_flags", "text", True), *SRC],
        pk=["sandbox_profile_id", "policy_version"], fks=[SRC_FK],
        comment="one row per policy version, flattened in DuckDB; validity dates are ordinary columns"),
    Table("policy_allowed_tool", [
        C("sandbox_profile_id", "text", True), C("policy_version", "integer", True), C("tool_id", "text", True)],
        pk=["sandbox_profile_id", "policy_version", "tool_id"],
        fks=[(["sandbox_profile_id", "policy_version"], "dim_policy", ["sandbox_profile_id", "policy_version"])]),
    Table("policy_egress_rule", [
        C("sandbox_profile_id", "text", True), C("policy_version", "integer", True),
        C("allowlist_entry", "text", True), C("is_wildcard", "boolean", True), C("match_regex", "text", True)],
        pk=["sandbox_profile_id", "policy_version", "allowlist_entry"],
        fks=[(["sandbox_profile_id", "policy_version"], "dim_policy", ["sandbox_profile_id", "policy_version"])]),
    Table("dim_model_price", [
        C("model_id", "text", True), C("effective_from_utc", "timestamptz", True), C("effective_to_utc", "timestamptz"),
        C("model_family", "text", True), C("input_price_per_1k", "numeric(12,6)"),
        C("output_price_per_1k", "numeric(12,6)"), C("currency", "text", True), C("quality_flags", "text", True), *SRC],
        pk=["model_id", "effective_from_utc"], fks=[SRC_FK]),

    Table("fact_session", [
        C("session_id", "text", True), C("tenant_id", "text", True), C("agent_id", "text", True),
        C("release_version", "text", True), C("agent_sk", "integer", True), C("sandbox_profile_id", "text"),
        C("policy_version_applied", "integer"), C("sandbox_instance_id", "text", True), C("task_id", "text", True),
        C("task_category", "text", True), C("expected_tool_categories", "text", True),
        C("start_time_utc", "timestamptz", True), C("date_key", "integer", True), C("time_key", "smallint", True),
        C("end_time_utc", "timestamptz"), C("terminal_event_type", "text"), C("outcome", "text"),
        C("duration_s", "double precision"), C("terminal_latency_ms", "bigint"), C("latency_disagrees", "boolean"),
        C("n_events", "integer", True), C("n_prompt_turns", "integer", True), C("n_tool_calls", "integer", True),
        C("n_tool_violations", "integer", True), C("n_tool_denied", "integer", True), C("n_tool_failed", "integer", True),
        C("n_network_flows", "integer", True), C("n_egress_violations", "integer", True),
        C("n_distinct_destinations", "integer", True), C("n_sandbox_decisions", "integer", True),
        C("n_sandbox_denials", "integer", True), C("n_escalation_attempts", "integer", True),
        C("n_escalation_denied", "integer", True), C("n_injection_turns", "integer", True),
        C("n_late_events", "integer", True), C("has_sequence_gap", "boolean", True),
        C("is_trace_complete", "boolean", True), C("executed_tool_categories", "text", True),
        C("is_goal_drift", "boolean"), C("first_injection_time_utc", "timestamptz"),
        C("first_violation_time_utc", "timestamptz"), C("first_violation_after_injection_utc", "timestamptz"),
        C("tokens_in", "bigint"), C("tokens_out", "bigint"), C("cost_usd", "numeric(14,6)"),
        C("start_activity_id", "text", True), C("terminal_activity_id", "text"), *SRC],
        pk=["session_id"], fks=[TENANT_FK, AGENT_FK, POLICY_FK, DATE_FK, TIME_FK, SRC_FK],
        indexes=[["tenant_id"], ["agent_id", "release_version"], ["date_key"]],
        comment="one row per accepted session (accumulating snapshot of the whole session)"),
    Table("fact_prompt_turn", [
        C("event_id", "text", True), C("activity_id", "text", True), *WHO,
        C("event_time_utc", "timestamptz", True), C("ingest_time_utc", "timestamptz", True),
        C("lateness_s", "double precision", True), C("is_late", "boolean", True), *WHEN,
        C("sequence_number", "integer", True), C("turn_index", "integer", True), C("status", "text", True),
        C("latency_ms", "bigint", True), C("model_id", "text", True), C("content_source", "text", True),
        C("stated_goal", "text", True), C("injection_indicators", "text", True),
        C("n_injection_indicators", "smallint", True), C("finish_reason", "text", True),
        C("tokens_in", "bigint", True), C("tokens_out", "bigint"), C("input_price_per_1k", "numeric(12,6)"),
        C("output_price_per_1k", "numeric(12,6)"), C("cost_usd", "numeric(14,6)"),
        C("event_time_format", "text", True), C("quality_flags", "text", True), *SRC],
        pk=["event_id"], fks=FACT_FKS, indexes=[["session_id"], ["date_key"], ["agent_id", "release_version"]]),
    Table("fact_tool_call", [
        C("event_id", "text", True), C("activity_id", "text", True), *WHO,
        C("event_time_utc", "timestamptz", True), C("ingest_time_utc", "timestamptz", True),
        C("lateness_s", "double precision", True), C("is_late", "boolean", True), *WHEN,
        C("sequence_number", "integer", True), C("tool_id", "text", True), C("tool_version", "text", True),
        C("tool_id_raw", "text", True), C("status", "text", True), C("status_raw", "text", True),
        C("error_class", "text"), C("retry_number", "integer", True), C("latency_ms", "bigint", True),
        C("finished_at_utc", "timestamptz", True), C("target_resource", "text", True), C("bytes_out", "bigint"),
        C("is_policy_violation", "boolean"), C("risk_indicators", "text", True), C("quality_flags", "text", True),
        *SRC],
        pk=["event_id"], fks=FACT_FKS + [(["tool_id", "tool_version"], "dim_tool", ["tool_id", "tool_version"])],
        indexes=[["session_id"], ["date_key"], ["agent_id", "release_version"], ["tool_id", "tool_version"]]),
    Table("fact_network_flow", [
        C("flow_id", "text", True), C("activity_id", "text", True), *WHO,
        C("flow_time_utc", "timestamptz", True), *WHEN, C("sandbox_instance_id", "text", True),
        C("src_ip", "text", True), C("dst_host", "text"), C("dst_host_raw", "text"), C("dst_ip", "text", True),
        C("dst_port", "integer", True), C("protocol", "text", True), C("bytes_sent", "bigint", True),
        C("bytes_received", "bigint", True), C("verdict", "text", True), C("geo_country", "text"),
        C("is_policy_violation", "boolean"), C("violation_rule", "text", True), C("risk_indicators", "text", True),
        C("quality_flags", "text", True), *SRC],
        pk=["flow_id"], fks=FACT_FKS, indexes=[["session_id"], ["src_ip", "dst_ip", "dst_port"]]),
    Table("fact_sandbox_decision", [
        C("audit_id", "text", True), C("activity_id", "text", True), *WHO,
        C("event_time_utc", "timestamptz", True), *WHEN, C("log_time_raw", "text", True), C("host", "text", True),
        C("host_offset_s", "double precision", True), C("process", "text", True), C("pid", "integer", True),
        C("sandbox_instance_id", "text", True), C("decision", "text", True), C("action", "text", True),
        C("is_escalation_action", "boolean", True), C("target", "text", True), C("reason", "text", True),
        C("policy_version_reported", "integer"), C("is_policy_violation", "boolean"),
        C("violation_rule", "text", True), C("risk_indicators", "text", True), C("quality_flags", "text", True), *SRC],
        pk=["audit_id"], fks=FACT_FKS, indexes=[["session_id"], ["date_key"]]),
    Table("fact_siem_alert", [
        C("alert_id", "text", True), C("activity_id", "text", True), C("detected_at_utc", "timestamptz", True),
        *WHEN, C("detected_at_raw", "text", True), C("utc_offset", "text", True), C("source_system", "text", True),
        C("severity", "text", True), C("source_ip", "text", True), C("destination_host", "text", True),
        C("destination_port", "integer", True), C("technique_id", "text", True), C("technique_name", "text"),
        C("rule", "text", True), C("confidence", "double precision", True), C("match_status", "text", True),
        C("n_candidate_flows", "integer", True), C("n_candidate_sessions", "integer", True),
        C("matched_flow_id", "text"), C("session_id", "text"), C("session_resolution", "text", True),
        C("delta_s", "double precision"), C("tenant_id", "text"), C("agent_id", "text"), C("release_version", "text"),
        C("quality_flags", "text", True), *SRC],
        pk=["alert_id"],
        fks=[SESSION_FK, TENANT_FK, AGENT_FK, DATE_FK, TIME_FK, SRC_FK,
             (["matched_flow_id"], "fact_network_flow", ["flow_id"])],
        indexes=[["session_id"], ["source_ip", "destination_host", "destination_port"]],
        comment="severity is the targeted system's opinion, carried as received"),
    Table("fact_rejected_record", [
        C("source_file", "text", True), C("source_line", "bigint", True), C("source_file_id", "text", True),
        C("source_format", "text", True), C("record_id", "text"), C("reason", "text", True, _reason_check),
        C("reason_detail", "text"), C("raw_payload", "text", True), C("zone", "text", True),
        C("channel", "text", True), C("detected_by", "text", True), C("kafka_topic", "text"),
        C("kafka_partition", "integer"), C("kafka_offset", "bigint")],
        pk=["source_file", "source_line"], fks=[SRC_FK], indexes=[["reason"], ["channel"]],
        prune_where="channel = 'batch'",
        comment="every refused record (batch and stream), exactly one enum reason, pointer to file and line"),
]
TABLE = {t.name: t for t in TABLES}
FACTS = [t.name for t in TABLES if t.name.startswith("fact_")]

# streaming tables are written by the consumer / window job, not by the batch load
STREAM_DDL = """
CREATE TABLE IF NOT EXISTS {s}.stream_curated_event (
    event_id text PRIMARY KEY,
    event_type text NOT NULL,
    session_id text NOT NULL,
    sequence_number integer NOT NULL,
    event_time_utc timestamptz NOT NULL,
    ingest_time_utc timestamptz NOT NULL,
    lateness_s double precision NOT NULL,
    is_late boolean NOT NULL,
    tenant_id text NOT NULL REFERENCES {s}.dim_tenant (tenant_id),
    agent_id text NOT NULL,
    release_version text NOT NULL,
    agent_sk integer,
    sandbox_instance_id text NOT NULL,
    status text NOT NULL,
    latency_ms bigint,
    model_id text,
    content_source text,
    stated_goal_fingerprint text,
    injection_indicators text,
    tokens_in bigint,
    tokens_out bigint,
    finish_reason text,
    tool_id text,
    tool_version text,
    finished_at_utc timestamptz,
    retry_number integer,
    error_class text,
    target_resource text,
    bytes_out bigint,
    task_id text,
    task_category text,
    expected_tool_categories text,
    quality_flags text NOT NULL,
    source_file text NOT NULL,
    source_line bigint NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    FOREIGN KEY (agent_id, release_version) REFERENCES {s}.dim_agent (agent_id, release_version)
);
CREATE TABLE IF NOT EXISTS {s}.stream_message_log (
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    event_id text,
    session_id text,
    source_line bigint,
    outcome text NOT NULL,
    reason text,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);
CREATE TABLE IF NOT EXISTS {s}.stream_tenant_minute (
    tenant_id text NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    events bigint NOT NULL,
    tool_calls bigint NOT NULL,
    denied_tool_calls bigint NOT NULL,
    is_final boolean NOT NULL,
    late_updates integer NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, window_start)
);
"""


def ddl(schema: str) -> str:
    """CREATE statements for every table of the spec, in dependency order."""
    parts = [f"CREATE SCHEMA IF NOT EXISTS {schema};"]
    for t in TABLES:
        lines = []
        for c in t.cols:
            s = f"    {c.name} {c.type}"
            if c.not_null:
                s += " NOT NULL"
            if c.check:
                s += f" CHECK ({c.check})"
            lines.append(s)
        lines.append(f"    loaded_at timestamptz NOT NULL DEFAULT now()")
        lines.append(f"    PRIMARY KEY ({', '.join(t.pk)})")
        for cols, ref, refcols in t.fks:
            lines.append(f"    FOREIGN KEY ({', '.join(cols)}) REFERENCES {schema}.{ref} ({', '.join(refcols)})")
        body = ",\n".join(lines)
        comment = f"-- {t.comment}\n" if t.comment else ""
        parts.append(f"{comment}CREATE TABLE IF NOT EXISTS {schema}.{t.name} (\n{body}\n);")
        for i, cols in enumerate(t.indexes):
            parts.append(f"CREATE INDEX IF NOT EXISTS {t.name}_ix{i} ON {schema}.{t.name} ({', '.join(cols)});")
    parts.append(STREAM_DDL.format(s=schema).strip())
    return "\n\n".join(parts) + "\n"


def columns_markdown() -> str:
    """Every warehouse column (type, nullability, key role), rendered from the spec."""
    lines = ["# Warehouse columns (schema `kestrel_dw`)", "",
             "Generated from `kestrel_m2/star.py` by `python -m kestrel_m2.warehouse --print-ddl`. The meaning of "
             "the columns and the policies behind them are in [`data_dictionary.md`](data_dictionary.md). Every "
             "table also has `loaded_at` (set when the row was inserted or last changed by the upsert).", ""]
    for t in TABLES:
        fk_of = {c: f"{ref}.{rc}" for cols, ref, refcols in t.fks for c, rc in zip(cols, refcols)}
        lines += [f"## `{t.name}`", "", t.comment or "", "",
                  f"Primary key (natural key of the upsert): `({', '.join(t.pk)})`", "",
                  "| column | type | null | key |", "|---|---|---|---|"]
        for c in t.cols:
            key = "PK" if c.name in t.pk else ""
            if c.name in fk_of:
                key = (key + " " if key else "") + f"FK -> `{fk_of[c.name]}`"
            lines.append(f"| `{c.name}` | {c.type} | {'' if c.not_null else 'yes'} | {key} |")
        lines.append("")
    return "\n".join(lines)


# ========================================================================== build

def date_key(ts: pd.Series) -> pd.Series:
    return (ts.dt.year * 10000 + ts.dt.month * 100 + ts.dt.day).astype("int64")


def time_key(ts: pd.Series) -> pd.Series:
    return (ts.dt.hour * 60 + ts.dt.minute).astype("int64")


def build_dim_date(first: pd.Timestamp, last: pd.Timestamp) -> pd.DataFrame:
    days = pd.date_range(first.normalize(), last.normalize(), freq="D", tz="UTC")
    iso = days.isocalendar()
    return pd.DataFrame({
        "date_key": date_key(pd.Series(days)).values, "full_date": days.date, "year": days.year,
        "quarter": days.quarter, "month": days.month, "month_name": days.month_name(), "day": days.day,
        "day_of_week": days.dayofweek + 1, "day_name": days.day_name(), "iso_year": iso.year.values,
        "iso_week": iso.week.values, "is_weekend": days.dayofweek >= 5})


def build_dim_time() -> pd.DataFrame:
    m = np.arange(24 * 60)
    hour, minute = m // 60, m % 60
    band = np.select([(hour >= lo) & (hour < hi) for lo, hi, _ in HOUR_BANDS], [b for _, _, b in HOUR_BANDS], default="")
    return pd.DataFrame({"time_key": m, "hour": hour, "minute": minute,
                         "hhmm": [f"{h:02d}:{mm:02d}" for h, mm in zip(hour, minute)], "hour_band": band,
                         "is_business_hours": (hour >= 9) & (hour < 17)})


def _pipe_list(s: pd.Series) -> pd.Series:
    return s.fillna("").map(lambda v: [x for x in str(v).split("|") if x])


def _when(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df["date_key"] = date_key(df[col])
    df["time_key"] = time_key(df[col])
    return df


def tool_categories(tool_calls: pd.DataFrame, dim_tool: pd.DataFrame) -> pd.Series:
    """Category of each tool call from its own (tool_id, tool_version); categories differ
    per version for tl_code_exec (M1 TL3), so the version decides."""
    key = dim_tool.set_index(["tool_id", "tool_version"]).tool_category
    idx = pd.MultiIndex.from_arrays([tool_calls.tool_id, tool_calls.tool_version])
    return pd.Series(key.reindex(idx).values, index=tool_calls.index)


def build_star(t: dict[str, pd.DataFrame], catalog: pd.DataFrame, policy: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """All warehouse tables from the curated M1 tables, the catalogue and the DuckDB policies."""
    sfid = id_by_file(catalog)
    src = lambda df: df.assign(source_file_id=df.source_file.map(sfid))   # noqa: E731

    a = t["stg_agent_activity"].copy()
    a["activity_id"] = "act:" + a.event_id
    a = src(_when(a, "event_time_utc"))
    sess = t["session"].copy()
    agents = t["dim_agent"]

    # ---------------------------------------------------------------- dimensions
    tenants = src(t["dim_tenant"].copy())
    tenants["merged_tenant_ids"] = _pipe_list(tenants.merged_tenant_ids)
    dim_agent = src(agents.copy())
    dim_agent["launch_date"] = pd.to_datetime(dim_agent.launch_date)
    dim_tool = src(t["dim_tool"].copy())
    pol = policy["dim_policy"].copy()
    m1_lines = t["dim_policy"][["sandbox_profile_id", "policy_version", "source_file", "source_line"]].astype(
        {"policy_version": "int64"})
    pol = src(pol.merge(m1_lines, on=["sandbox_profile_id", "policy_version"], how="left"))
    price = src(t["dim_model_price"].copy())

    # --------------------------------------------------------------- session fact
    outcomes = session_outcomes(a).set_index("session_id")
    tc = a[a.event_type == "tool.call"].copy()
    tc["tool_category"] = tool_categories(tc, t["dim_tool"])
    turns = a[a.event_type == "llm.turn"].copy()
    turns["n_injection_indicators"] = _pipe_list(turns.injection_indicators).map(len)
    fl = t["stg_network_flows"].copy()
    au = t["stg_sandbox_audit"].copy()

    g_all = a.groupby("session_id")
    seq = g_all.sequence_number.agg(n_seq="nunique", min_seq="min", max_seq="max")
    n_events = g_all.size()
    has_gap = (seq.min_seq != 1) | (seq.max_seq != seq.n_seq) | (seq.n_seq != n_events)

    inj_turns = turns[turns.n_injection_indicators > 0]
    first_inj = inj_turns.groupby("session_id").event_time_utc.min()
    viol_times = pd.concat([tc.loc[tc.is_policy_violation.fillna(False).astype(bool), ["session_id", "event_time_utc"]],
                            fl.loc[fl.is_policy_violation.fillna(False).astype(bool), ["session_id", "flow_time_utc"]]
                            .rename(columns={"flow_time_utc": "event_time_utc"})])
    first_viol = viol_times.groupby("session_id").event_time_utc.min()
    v2 = viol_times.join(first_inj.rename("fi"), on="session_id")
    first_viol_after = v2[v2.event_time_utc >= v2.fi].groupby("session_id").event_time_utc.min()

    expected = sess.set_index("session_id").expected_tool_categories
    executed = tc.dropna(subset=["tool_category"]).groupby("session_id").tool_category.agg(lambda s: set(s))
    drift = pd.Series(pd.NA, index=sess.session_id, dtype="boolean")
    has_both = executed.index.intersection(expected.dropna().index)
    drift.loc[has_both] = [not executed[s] <= set(expected[s].split("|")) for s in has_both]

    start_rows = a[a.event_type == START].set_index("session_id")
    term_rows = a[a.event_type.isin(TERMINAL)].set_index("session_id")
    fs = sess.copy().set_index("session_id")
    fs["policy_version_applied"] = start_rows.policy_version_applied.reindex(fs.index)
    fs["date_key"] = date_key(fs.start_time_utc)
    fs["time_key"] = time_key(fs.start_time_utc)
    fs["end_time_utc"] = outcomes.end_time_utc.reindex(fs.index)
    fs["terminal_event_type"] = term_rows.event_type.reindex(fs.index)
    fs["outcome"] = outcomes.outcome.reindex(fs.index)
    fs["duration_s"] = outcomes.duration_s.reindex(fs.index)
    fs["terminal_latency_ms"] = term_rows.latency_ms.reindex(fs.index)
    fs["latency_disagrees"] = outcomes.latency_disagrees.reindex(fs.index).astype("boolean")
    count = lambda df, col="session_id": df.groupby(col).size().reindex(fs.index, fill_value=0)  # noqa: E731
    fs["n_events"] = n_events.reindex(fs.index, fill_value=0)
    fs["n_prompt_turns"] = count(turns)
    fs["n_tool_calls"] = count(tc)
    fs["n_tool_violations"] = count(tc[tc.is_policy_violation.fillna(False).astype(bool)])
    fs["n_tool_denied"] = count(tc[tc.status == "denied"])
    fs["n_tool_failed"] = count(tc[tc.status.isin(["failure", "timeout"])])
    fs["n_network_flows"] = count(fl)
    fs["n_egress_violations"] = count(fl[fl.is_policy_violation.fillna(False).astype(bool)])
    fs["n_distinct_destinations"] = (fl.assign(dst=fl.dst_host.fillna(fl.dst_ip)).groupby("session_id").dst.nunique()
                                     .reindex(fs.index, fill_value=0))
    fs["n_sandbox_decisions"] = count(au)
    fs["n_sandbox_denials"] = count(au[au.decision == "DENY"])
    esc = au[au.action.isin(ESCALATION_ACTIONS)]
    fs["n_escalation_attempts"] = count(esc)
    fs["n_escalation_denied"] = count(esc[esc.decision == "DENY"])
    fs["n_injection_turns"] = count(inj_turns)
    fs["n_late_events"] = count(a[a.is_late])
    fs["has_sequence_gap"] = has_gap.reindex(fs.index).fillna(True).astype(bool)
    fs["is_trace_complete"] = outcomes.trace_complete.reindex(fs.index).fillna(False).astype(bool)
    fs["executed_tool_categories"] = executed.reindex(fs.index).map(
        lambda v: "|".join(sorted(v)) if isinstance(v, set) else "")
    fs["is_goal_drift"] = drift.reindex(fs.index)
    fs["first_injection_time_utc"] = first_inj.reindex(fs.index)
    fs["first_violation_time_utc"] = first_viol.reindex(fs.index)
    fs["first_violation_after_injection_utc"] = first_viol_after.reindex(fs.index)
    tok = turns.groupby("session_id").agg(tokens_in=("tokens_in", "sum"),
                                          tokens_out=("tokens_out", lambda s: s.sum(min_count=1)),
                                          cost_usd=("cost_usd", lambda s: s.sum(min_count=1)))
    for c in ("tokens_in", "tokens_out", "cost_usd"):
        fs[c] = tok[c].reindex(fs.index)
    fs["cost_usd"] = fs.cost_usd.astype("Float64").round(6)
    fs["start_activity_id"] = start_rows.activity_id.reindex(fs.index)
    fs["terminal_activity_id"] = term_rows.activity_id.reindex(fs.index)
    fs["source_file_id"] = start_rows.source_file_id.reindex(fs.index)
    fs["source_line"] = start_rows.source_line.reindex(fs.index)
    fact_session = fs.reset_index()

    # ------------------------------------------------------------- event facts
    s_agent = sess.set_index("session_id")[["agent_id", "release_version"]]
    fact_prompt_turn = turns
    fact_tool_call = tc

    f = _when(src(fl), "flow_time_utc")
    f["activity_id"] = "flow:" + f.flow_id
    f = f.join(s_agent, on="session_id")

    u = _when(src(au), "event_time_utc")
    u["activity_id"] = "audit:" + u.audit_id
    u["is_escalation_action"] = u.action.isin(ESCALATION_ACTIONS)
    u = u.join(s_agent, on="session_id")

    corr = t["siem_correlation"].rename(columns={"flow_id": "matched_flow_id"})
    al = t["stg_siem_alerts"].merge(corr, on="alert_id", how="left", validate="one_to_one")
    al = _when(src(al), "detected_at_utc")
    al["activity_id"] = "siem:" + al.alert_id
    al = al.join(sess.set_index("session_id")[["tenant_id", "agent_id", "release_version"]], on="session_id")
    flags = pd.Series("", index=al.index, dtype="string")
    flags = flags.mask(al.session_resolution == "single_session", "CORRELATION_AMBIGUOUS_FLOW_SINGLE_SESSION")
    flags = flags.mask((al.match_status == "ambiguous") & al.session_id.isna(), "CORRELATION_AMBIGUOUS")
    flags = flags.mask(al.match_status == "unmatched", "CORRELATION_UNMATCHED")
    al["quality_flags"] = flags

    rej = t["fact_rejected_record"].drop(columns=["reject_id"]).copy()
    rej = src(rej).assign(zone="raw", channel="batch", detected_by="pydantic_gate+m1_ladder",
                          kafka_topic=None, kafka_partition=pd.NA, kafka_offset=pd.NA)

    # ------------------------------------------------------------------ calendar
    times = [a.event_time_utc, fl.flow_time_utc, au.event_time_utc, al.detected_at_utc]
    first = min(x.min() for x in times)
    last = max(max(x.max() for x in times), pd.Timestamp(config.STREAM_END) - pd.Timedelta(days=1))

    out = {
        "dim_source_file": catalog,
        "dim_date": build_dim_date(first, last),
        "dim_time": build_dim_time(),
        "dim_tenant": tenants,
        "dim_agent": dim_agent,
        "dim_tool": dim_tool,
        "dim_policy": pol,
        "policy_allowed_tool": policy["policy_allowed_tool"],
        "policy_egress_rule": policy["policy_egress_rule"],
        "dim_model_price": price,
        "fact_session": fact_session,
        "fact_prompt_turn": fact_prompt_turn,
        "fact_tool_call": fact_tool_call,
        "fact_network_flow": f,
        "fact_sandbox_decision": u,
        "fact_siem_alert": al,
        "fact_rejected_record": rej,
    }
    return {name: shape(name, df) for name, df in out.items()}


# pandas dtype per Postgres type, so every frame has the exact types the schema expects
_DTYPE = {"text": "string", "char(64)": "string", "char(5)": "string", "bigint": "Int64", "integer": "Int64",
          "smallint": "Int64", "double precision": "Float64", "numeric(14,6)": "Float64",
          "numeric(12,6)": "Float64", "boolean": "boolean", "timestamptz": "datetime64[ms, UTC]",
          "date": "datetime64[ms]", "text[]": "object", "integer[]": "object"}


def shape(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Select the spec's columns in order and cast to the spec's types."""
    spec = TABLE[name]
    df = df.rename(columns={"network_egress_allowlist": "egress_allowlist"})
    missing = [c for c in spec.columns if c not in df.columns]
    if missing:
        raise KeyError(f"{name}: builder did not produce {missing}")
    out = df[spec.columns].copy()
    for c in spec.cols:
        want = _DTYPE[c.type]
        if want == "datetime64[ms, UTC]":
            out[c.name] = pd.to_datetime(out[c.name], utc=True).astype(want)
        elif want == "datetime64[ms]":
            out[c.name] = pd.to_datetime(out[c.name]).astype(want)
        elif want != "object":
            out[c.name] = out[c.name].astype(want)
    if spec.name in ("fact_prompt_turn", "fact_session", "dim_model_price"):
        for c in ("cost_usd", "input_price_per_1k", "output_price_per_1k"):
            if c in out:
                out[c] = out[c].round(6)
    return out.reset_index(drop=True)
