"""Stage 2 output: every accepted record in the one canonical row shape (Dataset Description §5).

Six activity types from four sources:

=================  ======================  ==================  =========================
activity_type      source                  activity_subtype    activity_id
=================  ======================  ==================  =========================
lifecycle          agent_activity.parquet  event_type          ``act:<event_id>``
prompt_turn        agent_activity.parquet  ``llm.turn``        ``act:<event_id>``
tool_call          agent_activity.parquet  ``tool.call``       ``act:<event_id>``
network_flow       network_flows.csv       protocol            ``flow:<flow_id>``
sandbox_decision   sandbox_audit.log       action              ``audit:L<line>``
siem_alert         siem_alerts.json        rule                ``siem:<alert_id>``
=================  ======================  ==================  =========================

``status`` is the source's own outcome detail: activity status (ok/error, success/
failure/denied/timeout), the sandbox's reason (EGRESS_ALLOWED, PORT_DENIED, ...), the
SIEM correlation status (matched/ambiguous/unmatched). Flows have none.
``verdict`` is a single access vocabulary across sources: ``allowed | denied |
blocked | logged_only`` (tool calls: denied when the platform denied them, else allowed;
sandbox: ALLOW/DENY; flows: as written). Lifecycle rows, prompt turns and alerts have
no verdict.

Rows without their own session context (flows, audit lines, alerts) inherit tenant,
agent_sk and task from the accepted session they belong to. For alerts, that is the
session found by correlation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .reference import add_flag
from .timeutil import UTC_DTYPE

CANONICAL_COLUMNS = [
    "activity_id", "source_file", "source_format", "source_line",
    "event_time_utc", "ingest_time_utc", "lateness_s",
    "tenant_id", "agent_sk", "session_id", "task_id",
    "activity_type", "activity_subtype", "status", "verdict", "target_resource",
    "policy_version_applied", "is_policy_violation", "risk_indicators",
    "tokens_in", "tokens_out", "cost_usd", "quality_flags",
]

ACTIVITY_TYPE = {"session.start": "lifecycle", "session.end": "lifecycle", "session.error": "lifecycle",
                 "llm.turn": "prompt_turn", "tool.call": "tool_call"}

DTYPES = {
    "activity_id": "string", "source_file": "string", "source_format": "string", "source_line": "int64",
    "event_time_utc": UTC_DTYPE, "ingest_time_utc": UTC_DTYPE, "lateness_s": "Float64",
    "tenant_id": "string", "agent_sk": "Int64", "session_id": "string", "task_id": "string",
    "activity_type": "string", "activity_subtype": "string", "status": "string", "verdict": "string",
    "target_resource": "string", "policy_version_applied": "Int64", "is_policy_violation": "boolean",
    "risk_indicators": "string", "tokens_in": "Int64", "tokens_out": "Int64", "cost_usd": "Float64",
    "quality_flags": "string",
}


def _shape(df: pd.DataFrame) -> pd.DataFrame:
    for c in CANONICAL_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[CANONICAL_COLUMNS].astype(DTYPES)


def _from_session(df: pd.DataFrame, sessions: pd.DataFrame, cols=("tenant_id", "agent_sk", "task_id")) -> pd.DataFrame:
    s = sessions.set_index("session_id")
    for c in cols:
        df[c] = df.session_id.map(s[c])
    return df


def from_activity(a: pd.DataFrame) -> pd.DataFrame:
    is_tool = a.event_type == "tool.call"
    return _shape(pd.DataFrame({
        "activity_id": "act:" + a.event_id,
        "source_file": a.source_file, "source_format": "parquet", "source_line": a.source_line,
        "event_time_utc": a.event_time_utc, "ingest_time_utc": a.ingest_time_utc, "lateness_s": a.lateness_s,
        "tenant_id": a.tenant_id, "agent_sk": a.agent_sk, "session_id": a.session_id, "task_id": a.task_id,
        "activity_type": a.event_type.map(ACTIVITY_TYPE), "activity_subtype": a.event_type,
        "status": a.status,
        "verdict": pd.Series(np.where(a.status == "denied", "denied", "allowed"), index=a.index).where(is_tool),
        "target_resource": a.target_resource.where(is_tool),
        "policy_version_applied": a.policy_version_applied, "is_policy_violation": a.is_policy_violation,
        "risk_indicators": a.risk_indicators, "tokens_in": a.tokens_in, "tokens_out": a.tokens_out,
        "cost_usd": a.cost_usd, "quality_flags": a.quality_flags,
    }))


def from_flows(f: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({
        "activity_id": "flow:" + f.flow_id,
        "source_file": f.source_file, "source_format": "csv", "source_line": f.source_line,
        "event_time_utc": f.flow_time_utc, "session_id": f.session_id,
        "activity_type": "network_flow", "activity_subtype": f.protocol,
        "verdict": f.verdict, "target_resource": f.target_resource,
        "policy_version_applied": f.policy_version_applied, "is_policy_violation": f.is_policy_violation,
        "risk_indicators": f.risk_indicators, "quality_flags": f.quality_flags,
    })
    return _shape(_from_session(df, sessions))


def from_audit(u: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({
        "activity_id": "audit:" + u.audit_id,
        "source_file": u.source_file, "source_format": "syslog", "source_line": u.source_line,
        "event_time_utc": u.event_time_utc, "session_id": u.session_id,
        "activity_type": "sandbox_decision", "activity_subtype": u.action,
        "status": u.reason, "verdict": u.decision.map({"ALLOW": "allowed", "DENY": "denied"}),
        "target_resource": u.target,
        "policy_version_applied": u.policy_version_applied, "is_policy_violation": u.is_policy_violation,
        "risk_indicators": u.risk_indicators, "quality_flags": u.quality_flags,
    })
    return _shape(_from_session(df, sessions))


def from_siem(s: pd.DataFrame, correlation: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """Alerts with the session found by correlation; ``status`` = matched/ambiguous/unmatched."""
    s = s.merge(correlation[["alert_id", "match_status", "session_id", "session_resolution"]], on="alert_id",
                how="left", validate="one_to_one")
    flags = pd.Series("", index=s.index)
    flags = add_flag(flags, s.session_resolution == "single_session", "CORRELATION_AMBIGUOUS_FLOW_SINGLE_SESSION")
    flags = add_flag(flags, (s.match_status == "ambiguous") & s.session_id.isna(), "CORRELATION_AMBIGUOUS")
    flags = add_flag(flags, s.match_status == "unmatched", "CORRELATION_UNMATCHED")
    df = pd.DataFrame({
        "activity_id": "siem:" + s.alert_id,
        "source_file": s.source_file, "source_format": "nested_json", "source_line": s.source_line,
        "event_time_utc": s.detected_at_utc, "session_id": s.session_id,
        "activity_type": "siem_alert", "activity_subtype": s.rule, "status": s.match_status,
        "target_resource": s.destination_host + ":" + s.destination_port.astype("string"),
        "risk_indicators": "siem_alert|technique:" + s.technique_id,
        "quality_flags": flags,
    })
    return _shape(_from_session(df, sessions))


def build_canonical(activity: pd.DataFrame, flows: pd.DataFrame, audit: pd.DataFrame, siem: pd.DataFrame,
                    correlation: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    """Union of all accepted, enriched records; checks the canonical invariants."""
    parts = [from_activity(activity), from_flows(flows, sessions), from_audit(audit, sessions),
             from_siem(siem, correlation, sessions)]
    out = pd.concat(parts, ignore_index=True)
    out = out.sort_values(["event_time_utc", "activity_id"], ignore_index=True)
    check_canonical(out)
    return out


def check_canonical(df: pd.DataFrame) -> None:
    assert list(df.columns) == CANONICAL_COLUMNS
    assert df.activity_id.is_unique, "duplicate activity_id"
    assert df.event_time_utc.notna().all(), "canonical row without event_time_utc"
    assert df.source_file.notna().all() and df.source_line.notna().all(), "row without provenance"
    assert df.activity_type.isin(["lifecycle", "prompt_turn", "tool_call", "network_flow", "sandbox_decision",
                                  "siem_alert"]).all()
    tokens = df.tokens_in.notna() | df.tokens_out.notna() | df.cost_usd.notna()
    assert (df.activity_type[tokens] == "prompt_turn").all(), "tokens/cost outside prompt turns"
