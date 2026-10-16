"""Stage 2 for network_flows.csv: validate, repair dst_host, link to the session.

flow_time is genuine UTC (profiling section 8: flows trail their causing tool call by
~0.3 s), so it is used as-is and is the clock anchor for the audit log.

Flows are "non-start events" of a session: a flow whose session has no accepted
``session.start`` is an ORPHAN_EVENT, exactly like an activity row.
"""

from __future__ import annotations

import pandas as pd

from . import readers
from .reference import add_flag
from .rejects import StageResult, build_rejects, check_rejects, first_reason, join_details, summarise
from .timeutil import parse_iso_utc

SOURCE_FORMAT = "csv"

REQUIRED = ["flow_id", "session_id", "sandbox_instance_id", "flow_time", "src_ip", "dst_ip", "dst_port", "protocol",
            "bytes_sent", "bytes_received", "verdict"]          # dst_host and geo_country are nullable
INT_COLS = ["dst_port", "bytes_sent", "bytes_received"]
NUMERIC_RANGE = {"dst_port": (1, 65535), "bytes_sent": (0, None), "bytes_received": (0, None)}
ENUMS = {"protocol": {"https", "http", "ssh", "smb", "dns", "rdp"},
         "verdict": {"allowed", "blocked", "logged_only"}}
IPV4_RX = r"(?:\d{1,3}\.){3}\d{1,3}"
PATTERNS = {"flow_id": r"nf_\w+", "session_id": r"ss_\w+", "src_ip": IPV4_RX, "dst_ip": IPV4_RX}

ACCEPTED_COLUMNS = [
    "source_file", "source_line", "flow_id", "session_id", "sandbox_instance_id", "flow_time_utc",
    "tenant_id", "agent_sk", "sandbox_profile_id", "src_ip", "dst_host", "dst_host_raw", "dst_ip", "dst_port",
    "target_resource", "protocol", "bytes_sent", "bytes_received", "verdict", "geo_country", "quality_flags",
]


def ip_host_map(df: pd.DataFrame) -> pd.Series:
    """dst_ip -> dst_host learnt from rows that have both, kept only where unambiguous."""
    pairs = df.dropna(subset=["dst_ip", "dst_host"]).drop_duplicates(["dst_ip", "dst_host"])
    one = pairs.groupby("dst_ip").dst_host.transform("size") == 1
    return pairs[one].set_index("dst_ip").dst_host


def _typed(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df["flow_time_utc"] = parse_iso_utc(df.flow_time)
    for c in INT_COLS:
        df[c + "_num"] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df


def judge(df: pd.DataFrame, session_ids: set) -> pd.DataFrame:
    idx = df.index
    checks = {}
    dup = df.flow_id.notna() & df.flow_id.duplicated(keep="first")      # no ingest_time: first in file wins
    checks["DUPLICATE_EVENT_ID"] = (dup, "flow_id repeats an earlier line")
    checks["MALFORMED_TIMESTAMP"] = (df.flow_time.notna() & df.flow_time_utc.isna(),
                                     "flow_time='" + df.flow_time.astype("string") + "'")
    orphan = ~df.session_id.isin(session_ids)
    checks["ORPHAN_EVENT"] = (orphan, "session " + df.session_id.fillna("<null>") + " has no accepted session.start")

    failed, detail = join_details({c: df[c].isna() for c in REQUIRED}, idx)
    checks["MISSING_REQUIRED_FIELD"] = (failed, "null: " + detail)

    out = {}
    for c, (lo, hi) in NUMERIC_RANGE.items():
        v = df[c + "_num"]
        out[c] = (v < lo) | ((v > hi) if hi is not None else False)
    failed, detail = join_details(out, idx)
    checks["INVALID_NUMERIC_RANGE"] = (failed, "out of range: " + detail)

    schema = {c: df[c].notna() & df[c + "_num"].isna() for c in INT_COLS}           # not an integer
    schema |= {c: df[c].notna() & ~df[c].isin(v) for c, v in ENUMS.items()}
    schema |= {c: df[c].notna() & ~df[c].str.fullmatch(rx).fillna(False) for c, rx in PATTERNS.items()}
    failed, detail = join_details(schema, idx)
    checks["SCHEMA_VIOLATION"] = (failed, "value outside contract: " + detail)
    return first_reason(checks, idx)


def clean_flows(sessions: pd.DataFrame, raw: pd.DataFrame | None = None) -> StageResult:
    """Validate flows against the accepted sessions (``activity.session_table``).

    Repairs: an empty dst_host is filled from the dst_ip -> host map learnt from the
    other flows when that IP has exactly one known host (flag DST_HOST_FILLED). The
    rest keep dst_host null and use the IP as target (flag DST_HOST_UNKNOWN):
    a hostname is never guessed.
    """
    raw = readers.read_network_flows() if raw is None else raw
    df = _typed(raw)
    verdict = judge(df, set(sessions.session_id))
    rejects = build_rejects(raw, verdict, SOURCE_FORMAT, "flow_id")

    a = df[verdict.reason.isna()].copy()
    learnt = ip_host_map(a)
    a["dst_host_raw"] = a.dst_host
    fill = a.dst_host.isna() & a.dst_ip.isin(learnt.index)
    a.loc[fill, "dst_host"] = a.loc[fill, "dst_ip"].map(learnt)
    for c in INT_COLS:
        a[c] = a[c + "_num"]
    a["target_resource"] = a.dst_host.fillna(a.dst_ip) + ":" + a.dst_port.astype("string")
    flags = pd.Series("", index=a.index)
    flags = add_flag(flags, fill, "DST_HOST_FILLED")
    flags = add_flag(flags, a.dst_host.isna(), "DST_HOST_UNKNOWN")
    a["quality_flags"] = flags

    s = sessions.set_index("session_id")
    for c in ["tenant_id", "agent_sk", "sandbox_profile_id"]:
        a[c] = a.session_id.map(s[c])
    a["agent_sk"] = a.agent_sk.astype("Int64")
    a = a[ACCEPTED_COLUMNS].sort_values(["flow_time_utc", "flow_id"]).reset_index(drop=True)

    check_rejects(rejects)
    assert a.flow_id.is_unique
    return StageResult(a, rejects, summarise(len(raw), len(a), rejects))
