"""Stage 2 for agent_activity.parquet: validate, deduplicate, repair, resolve keys.

Every one of the received rows ends up in exactly one of two places:

* ``accepted``: typed, UTC, keys resolved (tenant alias, agent_sk, tool alias), task
  propagated from the session's start, with every repair recorded in ``quality_flags``
* ``rejects``: the raw row as JSON plus file, line and exactly one enum reason

The reject ladder follows the enum order (``rejects.REASONS``). ORPHAN_EVENT depends
on which ``session.start`` rows were *accepted*, so the ladder runs twice: once to
judge the start rows on every other reason, then again with the orphan check added.
A start row is never an orphan, so the first pass decides its fate completely.
"""

from __future__ import annotations

import pandas as pd

from . import readers
from .reference import Reference, add_flag, attach_agent_sk, load_reference, resolve_tool_id
from .rejects import StageResult, build_rejects, check_rejects, first_reason, join_details, summarise
from .timeutil import UTC_DTYPE, is_late, lateness_seconds, parse_activity_time, parse_iso_utc

SOURCE_FORMAT = "parquet"

START = "session.start"
TERMINAL = {"session.end", "session.error"}
EVENT_TYPES = {START, "llm.turn", "tool.call"} | TERMINAL

# Dataset Description 3.5, column "FILLED ON"
REQUIRED_ALL = ["event_id", "event_time", "ingest_time", "sequence_number", "event_type", "session_id",
                "tenant_id", "agent_id", "release_version", "sandbox_instance_id", "status"]
REQUIRED_BY_TYPE = {
    START: ["task_id", "task_category", "expected_tool_categories"],
    "llm.turn": ["latency_ms", "turn_index", "model_id", "content_source", "stated_goal", "input_tokens",
                 "finish_reason"],
    "tool.call": ["latency_ms", "tool_id", "tool_version", "finished_at", "retry_number", "target_resource"],
    "session.end": ["latency_ms"],
    "session.error": ["latency_ms"],
}
# output_tokens is deliberately not required: missing-token policy = keep the turn,
# leave the count null (excluded from sums/averages, never imputed as 0), flag it.

# minimum allowed value per count column
NUMERIC_MIN = {"sequence_number": 1, "latency_ms": 0, "turn_index": 0, "input_tokens": 0, "output_tokens": 0,
               "retry_number": 0, "bytes_out": 0}

STATUS_BY_TYPE = {START: {"ok", "error"}, "llm.turn": {"ok", "error"}, "session.end": {"ok", "error"},
                  "session.error": {"ok", "error"}, "tool.call": {"success", "failure", "denied", "timeout"}}
STATUS_REPAIRS = {"succeeded": "success"}       # unambiguous synonym, repaired and flagged

ENUMS = {
    "content_source": {"user", "system", "retrieved_web", "tool_output"},
    "finish_reason": {"stop", "length", "tool_call", "content_filter"},
    "task_category": {"market_research", "ticket_triage", "report_generation", "data_query", "code_review",
                      "incident_response"},
    "error_class": {"TIMEOUT", "AUTH_DENIED", "RATE_LIMIT", "POLICY_DENIED", "TARGET_UNREACHABLE", "INVALID_ARGS"},
}
ID_PATTERNS = {"event_id": r"ev_\w+", "session_id": r"ss_\w+"}

SESSION_TASK_COLS = ["task_id", "task_category", "expected_tool_categories"]


ActivityResult = StageResult


# ----------------------------------------------------------------- normalisation

def normalise(raw: pd.DataFrame, ref: Reference) -> pd.DataFrame:
    """Parse times and resolve keys without deciding anything yet.

    Adds typed/resolved columns next to the raw ones; the raw columns are kept so the
    reject payload and the quality flags can show what was received.
    """
    df = raw.copy()
    t = parse_activity_time(df.event_time)
    df["event_time_utc"] = t.ts
    df["event_time_format"] = t.fmt
    df["ingest_time_utc"] = parse_iso_utc(df.ingest_time)
    df["finished_at_utc"] = parse_iso_utc(df.finished_at)
    df["lateness_s"] = lateness_seconds(df.ingest_time_utc, df.event_time_utc)

    df["tenant_id_resolved"] = df.tenant_id.replace(ref.tenant_alias)
    df = attach_agent_sk(df, ref.agents, "event_time_utc")

    tool_ids, rewritten = resolve_tool_id(df.tool_id, ref.tool_alias, set(ref.tools.tool_id))
    df["tool_id_resolved"] = tool_ids
    df["tool_alias_resolved"] = rewritten
    df["status_norm"] = df.status.replace(STATUS_REPAIRS)
    return df


# --------------------------------------------------------------------- the ladder

def _checks(df: pd.DataFrame, ref: Reference, watermark_s: float | None) -> dict:
    """Every reason except ORPHAN_EVENT, each as (failed mask, detail)."""
    idx = df.index
    checks = {}

    # DUPLICATE_EVENT_ID: keep the earliest ingest_time (source line breaks ties)
    order = df.sort_values(["ingest_time_utc", "source_line"], na_position="last")
    dup = order.event_id.notna() & order.event_id.duplicated(keep="first")
    kept_line = order[~dup].drop_duplicates("event_id").set_index("event_id").source_line
    dup = dup.reindex(idx)
    checks["DUPLICATE_EVENT_ID"] = (dup, "copy kept at line " + df.event_id.map(kept_line).astype("string")
                                    + " (earliest ingest_time)")

    checks["UNKNOWN_AGENT_ID"] = (df.agent_id.notna() & df.release_version.notna() & df.agent_sk.isna(),
                                  "(agent_id, release_version) = (" + df.agent_id + ", " + df.release_version
                                  + ") not in agents.csv")

    known_tenants = set(ref.tenants.tenant_id)
    checks["UNKNOWN_TENANT_ID"] = (df.tenant_id.notna() & ~df.tenant_id_resolved.isin(known_tenants),
                                   "tenant_id " + df.tenant_id + " not in tenants.csv")

    bad_event = df.event_time.notna() & df.event_time_utc.isna()
    bad_ingest = df.ingest_time.notna() & df.ingest_time_utc.isna()
    bad_finish = df.finished_at.notna() & df.finished_at_utc.isna()
    ts_detail = pd.Series("", index=idx, dtype="string")
    for col, m in {"event_time": bad_event, "ingest_time": bad_ingest, "finished_at": bad_finish}.items():
        piece = col + "='" + df[col].astype("string") + "'"
        ts_detail = ts_detail.where(~m, ts_detail.where(ts_detail == "", ts_detail + ",") + piece)
    checks["MALFORMED_TIMESTAMP"] = (bad_event | bad_ingest | bad_finish, ts_detail)

    checks["INVALID_EVENT_TYPE"] = (df.event_type.notna() & ~df.event_type.isin(EVENT_TYPES),
                                    "event_type '" + df.event_type + "' not in the event_type enum")

    missing = {c: df[c].isna() for c in REQUIRED_ALL}
    for etype, cols in REQUIRED_BY_TYPE.items():
        for c in cols:
            missing[c] = missing.get(c, pd.Series(False, index=idx)) | ((df.event_type == etype) & df[c].isna())
    failed, detail = join_details(missing, idx)
    checks["MISSING_REQUIRED_FIELD"] = (failed, "null: " + detail)

    below = {c: df[c] < lo for c, lo in NUMERIC_MIN.items()}
    failed, detail = join_details(below, idx)
    checks["INVALID_NUMERIC_RANGE"] = (failed, "below minimum: " + detail)

    if watermark_s is not None:
        checks["LATE_BEYOND_WATERMARK"] = (df.lateness_s > watermark_s,
                                           f"lateness > {watermark_s:g} s watermark on event_time")

    schema = {f"id:{c}": df[c].notna() & ~df[c].str.fullmatch(rx).fillna(False) for c, rx in ID_PATTERNS.items()}
    status_ok = pd.Series(False, index=idx)
    for etype, values in STATUS_BY_TYPE.items():
        status_ok |= (df.event_type == etype) & df.status_norm.isin(values)
    schema["status"] = df.status_norm.notna() & df.event_type.isin(EVENT_TYPES) & ~status_ok
    for c, values in ENUMS.items():
        schema[c] = df[c].notna() & ~df[c].isin(values)
    failed, detail = join_details(schema, idx)
    checks["SCHEMA_VIOLATION"] = (failed, "value outside contract: " + detail)
    return checks


def judge(df: pd.DataFrame, ref: Reference, watermark_s: float | None = None) -> pd.DataFrame:
    """Return ``reason`` / ``reason_detail`` per row (<NA> = accepted)."""
    checks = _checks(df, ref, watermark_s)
    first = first_reason(checks, df.index)

    accepted_start = (df.event_type == START) & first.reason.isna()
    sessions_with_start = set(df.session_id[accepted_start])
    orphan = (df.event_type != START) & ~df.session_id.isin(sessions_with_start)
    has_rejected_start = set(df.session_id[(df.event_type == START) & first.reason.notna()])
    why = pd.Series("session has no session.start", index=df.index, dtype="string")
    why[df.session_id.isin(has_rejected_start)] = "session.start of this session was rejected"
    checks["ORPHAN_EVENT"] = (orphan, why)
    return first_reason(checks, df.index)


# ---------------------------------------------------------------- accepted rows

def _quality_flags(df: pd.DataFrame) -> pd.Series:
    f = pd.Series("", index=df.index)
    f = add_flag(f, df.event_time_format.isin(["epoch_ms", "dmy_hms"]), "NONSTANDARD_TIMESTAMP_FORMAT")
    f = add_flag(f, df.release_version != df.release_version_norm, "VERSION_PREFIX_STRIPPED")
    f = add_flag(f, df.tenant_id != df.tenant_id_resolved, "TENANT_ALIAS_RESOLVED")
    f = add_flag(f, df.tool_alias_resolved, "TOOL_ALIAS_RESOLVED")
    f = add_flag(f, df.status != df.status_norm, "STATUS_NORMALISED")
    f = add_flag(f, df.is_late, "LATE")
    f = add_flag(f, (df.event_type == "llm.turn") & df.output_tokens.isna(), "OUTPUT_TOKENS_MISSING")
    f = add_flag(f, df.finished_at_utc < df.event_time_utc, "FINISHED_BEFORE_START")
    f = add_flag(f, df.agent_version_outside_window, "AGENT_VERSION_OUTSIDE_WINDOW")
    f = add_flag(f, df.agent_tenant_id.notna() & (df.tenant_id_resolved != df.agent_tenant_id),
                 "TENANT_DIFFERS_FROM_AGENT")
    f = add_flag(f, (df.status_norm == "success") & df.error_class.notna(), "SUCCESS_WITH_ERROR_CLASS")
    return f


ACCEPTED_COLUMNS = [
    "source_file", "source_line", "event_id", "event_type", "sequence_number", "session_id",
    "event_time_utc", "event_time_raw", "event_time_format", "ingest_time_utc", "lateness_s", "is_late",
    "tenant_id", "tenant_id_raw", "agent_id", "release_version", "release_version_raw", "agent_sk",
    "sandbox_profile_id", "sandbox_instance_id", "status", "status_raw", "latency_ms",
    "task_id", "task_category", "expected_tool_categories",
    "turn_index", "model_id", "content_source", "stated_goal", "injection_indicators", "input_tokens",
    "output_tokens", "finish_reason",
    "tool_id", "tool_id_raw", "tool_version", "finished_at_utc", "retry_number", "error_class", "target_resource",
    "bytes_out", "quality_flags",
]


def _accepted(df: pd.DataFrame) -> pd.DataFrame:
    a = df.copy()
    a["is_late"] = is_late(a.lateness_s)
    a["quality_flags"] = _quality_flags(a)

    # the session's task lives on its start row; every accepted row has an accepted start
    task = a.loc[a.event_type == START].set_index("session_id")[SESSION_TASK_COLS]
    for c in SESSION_TASK_COLS:
        a[c] = a.session_id.map(task[c])

    a = a.rename(columns={"event_time": "event_time_raw", "tenant_id": "tenant_id_raw", "tenant_id_resolved": "tenant_id",
                          "release_version": "release_version_raw", "release_version_norm": "release_version",
                          "tool_id": "tool_id_raw", "tool_id_resolved": "tool_id",
                          "status": "status_raw", "status_norm": "status"})
    a["event_time_utc"] = a.event_time_utc.astype(UTC_DTYPE)
    return a[ACCEPTED_COLUMNS].sort_values(["event_time_utc", "session_id", "sequence_number"]).reset_index(drop=True)


# ------------------------------------------------------------------------- entry

def clean_activity(raw: pd.DataFrame | None = None, ref: Reference | None = None,
                   watermark_s: float | None = None) -> ActivityResult:
    """Run the full activity stage.

    ``watermark_s=None`` (the batch default) applies no watermark: late rows are
    accepted and flagged LATE so the late-event rate can be measured. The M2 stream
    consumer passes 60 to reject events beyond the Kafka watermark.
    """
    raw = readers.read_activity() if raw is None else raw
    ref = load_reference() if ref is None else ref
    df = normalise(raw, ref)
    verdict = judge(df, ref, watermark_s)

    rejects = build_rejects(raw, verdict, SOURCE_FORMAT, "event_id")
    accepted = _accepted(df[verdict.reason.isna()])
    check_rejects(rejects)
    assert accepted.event_id.is_unique
    return StageResult(accepted, rejects, summarise(len(raw), len(accepted), rejects))


SESSION_COLUMNS = ["session_id", "tenant_id", "agent_id", "release_version", "agent_sk", "sandbox_profile_id",
                   "sandbox_instance_id", "task_id", "task_category", "expected_tool_categories", "start_time_utc"]


def session_table(accepted: pd.DataFrame) -> pd.DataFrame:
    """One row per session with an accepted ``session.start``.

    This is the parent every other session-keyed source (flows, audit log) is checked
    against for ORPHAN_EVENT, and where they inherit tenant, agent_sk and sandbox profile.
    The agent version cannot change inside a session (one container per session).
    """
    s = accepted[accepted.event_type == START].rename(columns={"event_time_utc": "start_time_utc"})
    return s[SESSION_COLUMNS].reset_index(drop=True)
