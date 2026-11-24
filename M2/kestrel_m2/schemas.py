"""Lab 4: Pandera schemas between pipeline zones, with quarantine instead of silent drops.

Zones::

    raw files --(Pydantic gate, M1 ladder)--> CURATED (M1 stg_* tables)
              --(Pandera curated schemas)----> star model
              --(Pandera warehouse schemas)--> WAREHOUSE (PostgreSQL kestrel_dw)

Each schema covers **types** (dtype per column), **ranges**, **uniqueness** (natural
key), **referential** integrity (values must exist in the validated parent table) and
**freshness** (the newest record is no older than ``MAX_STALENESS`` before the expected
end of the data, and nothing is from the future).

Every custom check is named ``<ENUM_REASON>:<what>``, and Pandera's built-in failures
are mapped too (``not_nullable`` -> MISSING_REQUIRED_FIELD, uniqueness ->
DUPLICATE_EVENT_ID). ``enforce`` validates lazily and then:

* a failure **on a row** quarantines that row with exactly one reason (the first in
  enum order among its failures) and removes it from the zone's output;
* a failure **of the table** (missing column, wrong dtype, stale data) cannot be pinned
  on a row, so it raises ``ZoneContractError`` and the pipeline stops (non-zero exit).

Nothing is dropped silently: quarantined rows go to ``fact_rejected_record`` with
``zone`` = curated / warehouse and ``detected_by`` = pandera.
"""

from __future__ import annotations

import json

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from kestrel_m1.rejects import REASONS

from . import config, star

PACK_START = pd.Timestamp(config.PACK_START)
PACK_END = pd.Timestamp(config.PACK_END)
MAX_STALENESS = pd.Timedelta(days=1)

BUILTIN_REASON = {"not_nullable": "MISSING_REQUIRED_FIELD", "field_uniqueness": "DUPLICATE_EVENT_ID",
                  "multiple_fields_uniqueness": "DUPLICATE_EVENT_ID"}


class ZoneContractError(RuntimeError):
    """A table-level contract break between zones: the pipeline must stop."""

    def __init__(self, zone: str, table: str, failures: pd.DataFrame):
        self.zone, self.table, self.failures = zone, table, failures
        lines = [f"  - {r.check} [{r.column}] {r.failure_case}" for r in failures.itertuples()]
        super().__init__(f"{zone}.{table}: {len(failures)} table-level contract failure(s)\n" + "\n".join(lines[:20]))


def rule(reason: str, what: str, fn, **kw) -> pa.Check:
    assert reason in REASONS or reason == "FRESHNESS", reason
    return pa.Check(fn, name=f"{reason}:{what}", **kw)


def reason_of_check(check: str) -> str:
    check = str(check)
    if check in BUILTIN_REASON:
        return BUILTIN_REASON[check]
    head = check.split(":", 1)[0]
    return head if head in REASONS else "SCHEMA_VIOLATION"


def keep_first_unique(cols: list[str], what: str = "natural key") -> pa.Check:
    """Later copies of a key fail (the first copy is kept), like the M1 dedup policy."""
    return rule("DUPLICATE_EVENT_ID", f"{what} ({', '.join(cols)}) repeats",
                lambda df: ~df.duplicated(cols, keep="first"))


def in_parent(cols: list[str], parent: pd.DataFrame, parent_cols: list[str], reason: str, what: str) -> pa.Check:
    """Referential check; rows with a null in the key are not checked (SQL MATCH SIMPLE)."""
    keys = pd.MultiIndex.from_frame(parent[parent_cols].dropna().astype("string"))

    def fn(df: pd.DataFrame) -> pd.Series:
        sub = df[cols].astype("string")
        has_null = sub.isna().any(axis=1)
        found = pd.MultiIndex.from_frame(sub.fillna("")).isin(keys)
        return pd.Series(found | has_null.values, index=df.index)
    return rule(reason, what, fn)


def in_window(col: str, lo: pd.Timestamp, hi: pd.Timestamp) -> pa.Check:
    return rule("MALFORMED_TIMESTAMP", f"{col} outside [{lo:%Y-%m-%d}, {hi:%Y-%m-%d})",
                lambda df: df[col].isna() | ((df[col] >= lo) & (df[col] < hi)))


def fresh(col: str, expected_end: pd.Timestamp = PACK_END, staleness: pd.Timedelta = MAX_STALENESS) -> pa.Check:
    """Table-level: the newest record must be within ``staleness`` of the expected end
    (a truncated or stale extract fails the run) and not after it."""
    def fn(df: pd.DataFrame) -> bool:
        ok = df[col][df[col] < expected_end]          # future rows are row-level failures (in_window)
        return bool(len(ok)) and ok.max() >= expected_end - staleness
    return rule("FRESHNESS", f"max({col}) within {staleness} before {expected_end:%Y-%m-%d %H:%M}", fn)


def nonneg(cols: list[str]) -> list[pa.Check]:
    return [rule("INVALID_NUMERIC_RANGE", f"{c} < 0", lambda df, c=c: df[c].isna() | (df[c] >= 0)) for c in cols]


def one_of(col: str, values, reason: str = "SCHEMA_VIOLATION") -> pa.Check:
    values = set(values)
    return rule(reason, f"{col} not in {sorted(values)}", lambda df: df[col].isna() | df[col].isin(values))


# ======================================================================== curated zone

EVENT_TYPES = ["session.start", "llm.turn", "tool.call", "session.end", "session.error"]
STATUS_BY_TYPE = {"session.start": {"ok", "error"}, "llm.turn": {"ok", "error"}, "session.end": {"ok", "error"},
                  "session.error": {"ok", "error"}, "tool.call": {"success", "failure", "denied", "timeout"}}


def _status_ok(df: pd.DataFrame) -> pd.Series:
    ok = pd.Series(False, index=df.index)
    for t, values in STATUS_BY_TYPE.items():
        ok |= (df.event_type == t) & df.status.isin(values)
    return ok


def curated_schemas(ref: dict[str, pd.DataFrame]) -> dict[str, pa.DataFrameSchema]:
    """Schemas for M1's curated stage tables; ``ref`` holds the validated dims + sessions."""
    ts = "datetime64[ms, UTC]"
    tenants, agents, sessions = ref["dim_tenant"], ref["dim_agent"], ref["session"]
    common = lambda key: [keep_first_unique([key])]   # noqa: E731
    session_fk = in_parent(["session_id"], sessions, ["session_id"], "ORPHAN_EVENT",
                           "session_id has no accepted session.start")
    return {
        "stg_agent_activity": pa.DataFrameSchema({
            "event_id": pa.Column(nullable=False, checks=rule("SCHEMA_VIOLATION", "event_id pattern",
                                                              lambda s: s.str.fullmatch(r"ev_\w+"))),
            "event_type": pa.Column(nullable=False),
            "event_time_utc": pa.Column(ts, nullable=False),
            "ingest_time_utc": pa.Column(ts, nullable=False),
            "sequence_number": pa.Column("Int64", nullable=False),
            "session_id": pa.Column(nullable=False),
            "tenant_id": pa.Column(nullable=False),
            "agent_id": pa.Column(nullable=False),
            "release_version": pa.Column(nullable=False),
            "status": pa.Column(nullable=False),
            "lateness_s": pa.Column("float64", nullable=False),
            "tokens_in": pa.Column("Int64", nullable=True), "tokens_out": pa.Column("Int64", nullable=True),
            "cost_usd": pa.Column("Float64", nullable=True),
        }, checks=common("event_id") + [
            one_of("event_type", EVENT_TYPES, "INVALID_EVENT_TYPE"),
            rule("SCHEMA_VIOLATION", "status not allowed for event_type", _status_ok),
            in_window("event_time_utc", PACK_START, PACK_END),
            rule("INVALID_NUMERIC_RANGE", "sequence_number < 1", lambda df: df.sequence_number >= 1),
            *nonneg(["lateness_s", "latency_ms", "tokens_in", "tokens_out", "cost_usd", "retry_number", "bytes_out"]),
            in_parent(["tenant_id"], tenants, ["tenant_id"], "UNKNOWN_TENANT_ID", "tenant_id not in dim_tenant"),
            in_parent(["agent_id", "release_version"], agents, ["agent_id", "release_version"], "UNKNOWN_AGENT_ID",
                      "(agent_id, release_version) not in dim_agent"),
            session_fk, fresh("event_time_utc"),
        ], name="curated.stg_agent_activity"),
        "stg_network_flows": pa.DataFrameSchema({
            "flow_id": pa.Column(nullable=False), "session_id": pa.Column(nullable=False),
            "flow_time_utc": pa.Column(ts, nullable=False), "dst_ip": pa.Column(nullable=False),
            "dst_port": pa.Column("Int64", nullable=False), "verdict": pa.Column(nullable=False),
        }, checks=common("flow_id") + [
            in_window("flow_time_utc", PACK_START, PACK_END),
            rule("INVALID_NUMERIC_RANGE", "dst_port outside 1..65535", lambda df: df.dst_port.between(1, 65535)),
            *nonneg(["bytes_sent", "bytes_received"]),
            one_of("verdict", ["allowed", "blocked", "logged_only"]),
            one_of("protocol", ["https", "http", "ssh", "smb", "dns", "rdp"]),
            session_fk, fresh("flow_time_utc"),
        ], name="curated.stg_network_flows"),
        "stg_sandbox_audit": pa.DataFrameSchema({
            "audit_id": pa.Column(nullable=False), "session_id": pa.Column(nullable=False),
            "event_time_utc": pa.Column(ts, nullable=False), "decision": pa.Column(nullable=False),
            "action": pa.Column(nullable=False),
        }, checks=common("audit_id") + [
            in_window("event_time_utc", PACK_START, PACK_END),
            one_of("decision", ["ALLOW", "DENY"]),
            one_of("action", ["socket_connect", "file_read", "file_write", "process_spawn", "setuid", "mount",
                              "ptrace"]),
            session_fk, fresh("event_time_utc"),
        ], name="curated.stg_sandbox_audit"),
        "stg_siem_alerts": pa.DataFrameSchema({
            "alert_id": pa.Column(nullable=False), "detected_at_utc": pa.Column(ts, nullable=False),
            "severity": pa.Column(nullable=False), "confidence": pa.Column("float64", nullable=False),
        }, checks=common("alert_id") + [
            in_window("detected_at_utc", PACK_START, PACK_END),
            one_of("severity", ["low", "medium", "high", "critical"]),
            rule("INVALID_NUMERIC_RANGE", "confidence outside 0..1", lambda df: df.confidence.between(0, 1)),
            rule("INVALID_NUMERIC_RANGE", "destination_port outside 1..65535",
                 lambda df: df.destination_port.between(1, 65535)),
            fresh("detected_at_utc"),
        ], name="curated.stg_siem_alerts"),
    }


# ====================================================================== warehouse zone

_PANDERA_DTYPE = {"string": "string", "Int64": "Int64", "Float64": "Float64", "boolean": "boolean",
                  "datetime64[ms, UTC]": "datetime64[ms, UTC]", "datetime64[ms]": "datetime64[ms]"}
RANGES = {"sequence_number": (1, None), "dst_port": (1, 65535), "destination_port": (1, 65535),
          "confidence": (0, 1), "turn_index": (0, None), "retry_number": (0, None)}
NONNEG = ["tokens_in", "tokens_out", "cost_usd", "latency_ms", "bytes_out", "bytes_sent", "bytes_received",
          "duration_s", "lateness_s", "input_price_per_1k", "output_price_per_1k", "size_bytes", "row_count",
          "n_events", "n_prompt_turns", "n_tool_calls", "n_network_flows", "n_sandbox_decisions"]
ENUMS = {"outcome": ["success", "error"], "verdict": ["allowed", "blocked", "logged_only"],
         "decision": ["ALLOW", "DENY"], "severity": ["low", "medium", "high", "critical"],
         "match_status": ["matched", "ambiguous", "unmatched"], "zone": ["raw", "curated", "warehouse", "stream"],
         "channel": ["batch", "stream"], "content_source": ["user", "system", "retrieved_web", "tool_output"],
         "finish_reason": ["stop", "length", "tool_call", "content_filter"]}
EVENT_TIME = {"fact_session": "start_time_utc", "fact_prompt_turn": "event_time_utc", "fact_tool_call": "event_time_utc",
              "fact_network_flow": "flow_time_utc", "fact_sandbox_decision": "event_time_utc",
              "fact_siem_alert": "detected_at_utc"}
FK_REASON = {"dim_tenant": "UNKNOWN_TENANT_ID", "dim_agent": "UNKNOWN_AGENT_ID", "fact_session": "ORPHAN_EVENT",
             "dim_date": "MALFORMED_TIMESTAMP", "dim_time": "MALFORMED_TIMESTAMP"}


def warehouse_schema(name: str, parents: dict[str, pd.DataFrame]) -> pa.DataFrameSchema:
    """Schema generated from ``star.TABLE[name]`` plus the range/enum/time/freshness rules."""
    spec = star.TABLE[name]
    columns = {}
    for c in spec.cols:
        dtype = _PANDERA_DTYPE.get(star._DTYPE[c.type])
        columns[c.name] = pa.Column(dtype, nullable=not c.not_null)
    checks = [keep_first_unique(spec.pk, "primary key")]
    for cols, ref, refcols in spec.fks:
        if ref in parents:
            checks.append(in_parent(cols, parents[ref], refcols, FK_REASON.get(ref, "SCHEMA_VIOLATION"),
                                    f"({', '.join(cols)}) not in {ref}"))
    names = set(spec.columns)
    for col, (lo, hi) in RANGES.items():
        if col in names:
            checks.append(rule("INVALID_NUMERIC_RANGE", f"{col} outside {lo}..{hi or 'inf'}",
                               lambda df, col=col, lo=lo, hi=hi: df[col].isna() | (
                                   (df[col] >= lo) & ((df[col] <= hi) if hi is not None else True))))
    checks += nonneg([c for c in NONNEG if c in names])
    checks += [one_of(c, v) for c, v in ENUMS.items() if c in names]
    if name in EVENT_TIME:
        col = EVENT_TIME[name]
        checks += [in_window(col, PACK_START, PACK_END), fresh(col)]
    if name == "fact_session":
        checks.append(rule("SCHEMA_VIOLATION", "terminal session needs end time and duration",
                           lambda df: df.outcome.isna() | (df.end_time_utc.notna() & df.duration_s.notna())))
    return pa.DataFrameSchema(columns, checks=checks, strict=True, ordered=True, name=f"warehouse.{name}")


# ========================================================================== enforcement

QUARANTINE_COLUMNS = [c.name for c in star.TABLE["fact_rejected_record"].cols]


def enforce(df: pd.DataFrame, schema: pa.DataFrameSchema, *, zone: str, table: str, key: list[str],
            file_of: dict[str, str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate ``df``; return (rows that passed, quarantined rows in fact_rejected_record shape)."""
    try:
        schema.validate(df, lazy=True)
        return df, pd.DataFrame(columns=QUARANTINE_COLUMNS)
    except SchemaErrors as e:
        fc = e.failure_cases.copy()
    fc["reason"] = fc.check.map(reason_of_check)
    table_level = fc[fc["index"].isna() | (fc.reason == "FRESHNESS")]
    if len(table_level):
        raise ZoneContractError(zone, table, table_level)
    fc["rank"] = fc.reason.map(REASONS.index)
    fc = fc.sort_values(["rank", "check"], kind="stable")
    first = fc.drop_duplicates("index").set_index("index")
    bad_idx = first.index
    bad = df.loc[bad_idx]
    source_file = bad["source_file"] if "source_file" in bad else bad["source_file_id"].map(
        lambda v: str(v).split("@")[0])
    detail = (f"pandera {zone}.{table}: " + first.check.map(str) + " [" + first.column.map(str) + "="
              + first.failure_case.map(str) + "]")
    q = pd.DataFrame({
        "source_file": source_file.values,
        "source_line": bad["source_line"].astype("int64").values,
        "source_file_id": (bad["source_file_id"].values if "source_file_id" in bad
                           else source_file.map(file_of or {}).values),
        "source_format": source_file.map(lambda f: f.rsplit(".", 1)[-1]).values,
        "record_id": bad[key].astype("string").agg("|".join, axis=1).values,
        "reason": first.reason.values,
        "reason_detail": detail.values,
        "raw_payload": [json.dumps(r, default=str) for r in bad.to_dict("records")],
        "zone": zone, "channel": "batch", "detected_by": "pandera",
        "kafka_topic": None, "kafka_partition": pd.NA, "kafka_offset": pd.NA,
    })
    return df.drop(index=bad_idx), q[QUARANTINE_COLUMNS]


CURATED_KEYS = {"stg_agent_activity": ["event_id"], "stg_network_flows": ["flow_id"], "stg_sandbox_audit": ["audit_id"],
                "stg_siem_alerts": ["alert_id"]}
# dependency order: a parent is validated (and possibly loses rows) before its children
WAREHOUSE_ORDER = [t.name for t in star.TABLES]


def enforce_curated(t: dict[str, pd.DataFrame], file_of: dict[str, str]) -> tuple[dict, pd.DataFrame]:
    ref = {"dim_tenant": t["dim_tenant"], "dim_agent": t["dim_agent"], "session": t["session"]}
    schemas = curated_schemas(ref)
    out, quarantine = dict(t), []
    for name, schema in schemas.items():
        out[name], q = enforce(t[name], schema, zone="curated", table=name, key=CURATED_KEYS[name], file_of=file_of)
        quarantine.append(q)
    return out, pd.concat(quarantine, ignore_index=True)


def enforce_warehouse(frames: dict[str, pd.DataFrame]) -> tuple[dict, pd.DataFrame]:
    out, quarantine = {}, []
    for name in WAREHOUSE_ORDER:
        schema = warehouse_schema(name, out)
        out[name], q = enforce(frames[name], schema, zone="warehouse", table=name, key=star.TABLE[name].pk)
        quarantine.append(q)
    return out, pd.concat([q for q in quarantine if len(q)], ignore_index=True) if any(len(q) for q in quarantine) \
        else pd.DataFrame(columns=QUARANTINE_COLUMNS)
