"""Stage 2 for sandbox_audit.log: UTC conversion, validation, link to the session.

Clock (profiling 9.1): every host writes local time UTC+2 with no DST, whatever region
its hostname names, plus a per-host skew of up to ~90 s. The year is not written; the
pack is 2026 (the April lines are the UTC+2 spill-over of 31 March evening). Each
host's offset is *measured* here by pairing its decisions with the accepted tool calls
that caused them (same session and target), so the conversion is re-derived from data
on every run rather than hard-coded. Hosts with no pair fall back to exactly UTC+2.

The syslog has no record id. A line is identified by its file line number
(``audit:L<line>``), and an exact repeat of an earlier line is a duplicate delivery.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from . import readers
from .reference import add_flag
from .rejects import StageResult, build_rejects, check_rejects, first_reason, join_details, summarise
from .timeutil import AUDIT_HOST_UTC_OFFSET_H, audit_local_to_utc, audit_offsets_from_tool_calls, parse_audit_local

SOURCE_FORMAT = "syslog"
NULL_MARKER = "-"

FIELDS = ["log_time", "host", "process", "pid", "session_id", "sandbox_instance_id", "decision", "action", "target",
          "reason", "policy_version"]
NULLABLE = {"policy_version"}
DECISIONS = {"ALLOW", "DENY"}
ACTIONS = {"socket_connect", "file_read", "file_write", "process_spawn", "setuid", "mount", "ptrace"}

ACCEPTED_COLUMNS = [
    "source_file", "source_line", "audit_id", "event_time_utc", "log_time_raw", "host", "host_offset_s", "process",
    "pid", "session_id", "sandbox_instance_id", "tenant_id", "agent_sk", "sandbox_profile_id", "decision", "action",
    "target", "reason", "policy_version_reported", "quality_flags",
]


@dataclass
class AuditResult(StageResult):
    host_offsets: pd.DataFrame = None      # per-host measured offset, for the data dictionary


def measure_host_offsets(parsed: pd.DataFrame, accepted_activity: pd.DataFrame) -> pd.DataFrame:
    calls = accepted_activity.loc[accepted_activity.event_type == "tool.call",
                                  ["session_id", "target_resource", "event_time_utc"]]
    return audit_offsets_from_tool_calls(parsed, calls.rename(columns={"event_time_utc": "event_ts"}))


def judge(df: pd.DataFrame, session_ids: set) -> pd.DataFrame:
    idx = df.index
    checks = {}
    first_line = df.groupby("raw_line").source_line.transform("min")
    checks["DUPLICATE_EVENT_ID"] = (df.source_line != first_line,
                                    "identical to line " + first_line.astype("string") + " (no record id: whole line is the key)")
    checks["MALFORMED_TIMESTAMP"] = (parse_audit_local(df.log_time).isna(), "log_time='" + df.log_time + "'")
    checks["ORPHAN_EVENT"] = (~df.session_id.isin(session_ids),
                              "session " + df.session_id + " has no accepted session.start")
    failed, detail = join_details({c: df[c] == NULL_MARKER for c in FIELDS if c not in NULLABLE}, idx)
    checks["MISSING_REQUIRED_FIELD"] = (failed, "'-' in: " + detail)
    schema = {"decision": ~df.decision.isin(DECISIONS), "action": ~df.action.isin(ACTIONS),
              "policy_version": (df.policy_version != NULL_MARKER) & ~df.policy_version.str.fullmatch(r"\d+")}
    failed, detail = join_details(schema, idx)
    checks["SCHEMA_VIOLATION"] = (failed, "value outside contract: " + detail)
    return first_reason(checks, idx)


def clean_audit(sessions: pd.DataFrame, accepted_activity: pd.DataFrame,
                log: readers.AuditLog | None = None) -> AuditResult:
    """Validate the audit log against the accepted sessions and convert it to UTC."""
    log = readers.read_sandbox_audit() if log is None else log
    parsed, unparsed = log.parsed, log.unparsed
    received = len(parsed) + len(unparsed)

    verdict = judge(parsed, set(sessions.session_id))
    unparsed_verdict = pd.DataFrame({"reason": "UNPARSEABLE_RECORD",
                                     "reason_detail": "line does not match the syslog layout (" + unparsed.problem + ")"},
                                    index=unparsed.index)
    rejects = pd.concat([build_rejects(parsed, verdict, SOURCE_FORMAT, None, payload_col="raw_line"),
                         build_rejects(unparsed, unparsed_verdict, SOURCE_FORMAT, None, payload_col="raw_line")],
                        ignore_index=True).sort_values("source_line", ignore_index=True)

    a = parsed[verdict.reason.isna()].copy()
    offsets = measure_host_offsets(a, accepted_activity)
    offset_s = offsets.set_index("host").median_offset_s.to_dict()
    a["host_offset_s"] = a.host.map(offset_s).astype("float")
    a["event_time_utc"] = audit_local_to_utc(a.log_time, a.host, offset_s)
    a["audit_id"] = "L" + a.source_line.astype("string")
    a["log_time_raw"] = a.log_time
    a["policy_version_reported"] = pd.to_numeric(a.policy_version.replace(NULL_MARKER, pd.NA)).astype("Int64")
    a["pid"] = a.pid.astype("int64")

    s = sessions.set_index("session_id")
    for c in ["tenant_id", "agent_sk", "sandbox_profile_id"]:
        a[c] = a.session_id.map(s[c])
    a["agent_sk"] = a.agent_sk.astype("Int64")

    flags = pd.Series("", index=a.index)
    flags = add_flag(flags, a.policy_version_reported.isna(), "POLICY_VERSION_NOT_LOGGED")
    flags = add_flag(flags, a.host_offset_s.isna(), "CLOCK_OFFSET_FALLBACK_UTC_PLUS_2")
    flags = add_flag(flags, a.sandbox_instance_id != a.session_id.map(s.sandbox_instance_id), "INSTANCE_DIFFERS_FROM_SESSION")
    a["quality_flags"] = flags
    a["host_offset_s"] = a.host_offset_s.fillna(AUDIT_HOST_UTC_OFFSET_H * 3600.0)
    a = a[ACCEPTED_COLUMNS].sort_values(["event_time_utc", "source_line"]).reset_index(drop=True)

    check_rejects(rejects)
    return AuditResult(a, rejects, summarise(received, len(a), rejects), host_offsets=offsets)
