"""Stage 2 time normalisation: every source's clock to tz-aware UTC.

Conventions established in notebooks/01_profiling.ipynb (sections 7.5, 8, 9.1, 10):

* agent_activity ``ingest_time`` / ``finished_at``: ISO-8601 UTC with ``Z``.
* agent_activity ``event_time``: UTC in every parseable format. Epoch values are
  milliseconds; slash dates are day-first (``dd/mm/yyyy HH:MM:SS``).
* network_flows ``flow_time``: genuine UTC.
* sandbox_audit.log ``log_time``: host-local UTC+2 with no DST on every host
  (whatever region the hostname names), no year (always 2026 in this pack), plus a
  per-host clock skew of up to ~90 s measured against two independent UTC anchors.
* siem_alerts ``detected_at``: ISO-8601 with an explicit, correct offset per record.

All parsed timestamps are returned as ``datetime64[ms, UTC]``: the sources carry at
most millisecond precision, and one resolution everywhere keeps comparisons and
concatenations free of dtype surprises.
"""

from __future__ import annotations

import pandas as pd

UTC_DTYPE = "datetime64[ms, UTC]"

LATE_THRESHOLD_S = 10            # Dataset Description, Appendix A: late = lateness > 10 s
AUDIT_HOST_UTC_OFFSET_H = 2      # measured: all sandbox hosts run UTC+2 (profiling 9.1)
AUDIT_LOG_YEAR = 2026            # pack window 2026-01-31 .. 2026-03-31 (+ April spill-over)

# event_time formats seen in agent_activity, in the order they are tried
ISO_RX = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})"
EPOCH_MS_RX = r"\d{13}"
DMY_RX = r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}"

FORMAT_ISO = "iso8601"
FORMAT_EPOCH_MS = "epoch_ms"
FORMAT_DMY = "dmy_hms"
FORMAT_UNKNOWN = "unknown"
FORMAT_NULL = "null"


def _empty_utc(index) -> pd.Series:
    return pd.Series(pd.NaT, index=index, dtype=UTC_DTYPE)


def parse_iso_utc(s: pd.Series) -> pd.Series:
    """Strict ISO-8601 with ``Z`` or an explicit offset -> UTC. Anything else -> NaT.

    Impossible calendar dates such as ``2026-02-30`` are NaT too (strict parse),
    which is what makes them detectable as MALFORMED_TIMESTAMP.
    """
    out = _empty_utc(s.index)
    ok = s.notna() & s.astype("string").str.fullmatch(ISO_RX).fillna(False)
    if ok.any():
        out[ok] = pd.to_datetime(s[ok], utc=True, format="ISO8601", errors="coerce").astype(UTC_DTYPE)
    return out


def classify_time_format(s: pd.Series) -> pd.Series:
    """Name the textual format of each raw timestamp (null-safe)."""
    text = s.astype("string")
    fmt = pd.Series(FORMAT_UNKNOWN, index=s.index, dtype="string")
    for name, rx in [(FORMAT_ISO, ISO_RX), (FORMAT_EPOCH_MS, EPOCH_MS_RX), (FORMAT_DMY, DMY_RX)]:
        fmt[text.str.fullmatch(rx).fillna(False)] = name
    fmt[s.isna()] = FORMAT_NULL
    return fmt


def parse_activity_time(s: pd.Series) -> pd.DataFrame:
    """Parse agent_activity ``event_time`` in any of its formats.

    Returns a frame with
    * ``ts``: UTC timestamp, NaT when the value cannot be read as a real instant
    * ``fmt``: the detected format (iso8601, epoch_ms, dmy_hms, unknown, null)
    * ``nonstandard``: True when a valid value was not in the canonical ISO form, which
      becomes the quality flag NONSTANDARD_TIMESTAMP_FORMAT (repaired, not rejected)
    """
    fmt = classify_time_format(s)
    ts = _empty_utc(s.index)

    m = fmt == FORMAT_ISO
    ts[m] = parse_iso_utc(s[m])

    m = fmt == FORMAT_EPOCH_MS
    if m.any():
        ts[m] = pd.to_datetime(s[m].astype("int64"), unit="ms", utc=True).astype(UTC_DTYPE)

    m = fmt == FORMAT_DMY
    if m.any():
        ts[m] = pd.to_datetime(s[m], format="%d/%m/%Y %H:%M:%S", utc=True, errors="coerce").astype(UTC_DTYPE)

    nonstandard = ts.notna() & (fmt != FORMAT_ISO)
    return pd.DataFrame({"ts": ts, "fmt": fmt, "nonstandard": nonstandard})


def parse_audit_local(log_time: pd.Series, year: int = AUDIT_LOG_YEAR) -> pd.Series:
    """``Mar 14 11:22:04`` -> naive local timestamp, with the year the syslog format drops.

    ``%d`` with a leading space (``Feb  1``) parses because strptime treats a space in
    the format as "any whitespace".
    """
    return pd.to_datetime(f"{year} " + log_time.astype("string"), format="%Y %b %d %H:%M:%S", errors="coerce")


def audit_local_to_utc(log_time: pd.Series, host: pd.Series | None = None, host_offset_s: dict | None = None,
                       year: int = AUDIT_LOG_YEAR) -> pd.Series:
    """Host-local audit time -> UTC.

    Each host's measured offset (``host_offset_s``, from ``audit_offsets_from_tool_calls``)
    is subtracted; hosts without a measurement fall back to the fixed UTC+2 convention.
    The per-host part corrects real clock skew of up to ~90 s that two independent
    anchors (flows and tool calls) agree on.
    """
    local = parse_audit_local(log_time, year)
    offset = pd.Series(float(AUDIT_HOST_UTC_OFFSET_H * 3600), index=log_time.index)
    if host is not None and host_offset_s:
        offset = host.map(host_offset_s).astype("float").fillna(offset)
    return (local - pd.to_timedelta(offset, unit="s")).dt.tz_localize("UTC").astype(UTC_DTYPE)


def _offset_table(pairs: pd.DataFrame, key: str) -> pd.DataFrame:
    # one pairing per audit line: the candidate closest to the documented +2 h
    pairs = pairs.reset_index(drop=True)
    best = pairs.loc[(pairs.offset_s - AUDIT_HOST_UTC_OFFSET_H * 3600).abs().groupby(pairs[key]).idxmin()]
    out = best.groupby("host").offset_s.agg(pairs="size", median_offset_s="median",
                                            p05_offset_s=lambda s: s.quantile(.05),
                                            p95_offset_s=lambda s: s.quantile(.95))
    out["skew_vs_utc_plus_2_s"] = out.median_offset_s - AUDIT_HOST_UTC_OFFSET_H * 3600
    return out.reset_index()


def audit_offsets_from_flows(audit: pd.DataFrame, flows: pd.DataFrame) -> pd.DataFrame:
    """Per-host offset from socket_connect decisions paired with (UTC) network flows.

    Pairs on (session_id, target = dst_host:port or dst_ip:port). Covers only hosts
    whose sandboxes make network connections.
    """
    a = audit[audit.action == "socket_connect"].assign(local=parse_audit_local(audit.log_time),
                                                         audit_line=audit.source_line)
    f = flows.assign(ref=parse_iso_utc(flows.flow_time).dt.tz_localize(None),
                     t_host=flows.dst_host + ":" + flows.dst_port.astype(str),
                     t_ip=flows.dst_ip + ":" + flows.dst_port.astype(str))
    pairs = pd.concat([a.merge(f, left_on=["session_id", "target"], right_on=["session_id", "t_host"]),
                       a.merge(f, left_on=["session_id", "target"], right_on=["session_id", "t_ip"])])
    pairs["offset_s"] = (pairs.local - pairs.ref).dt.total_seconds()
    return _offset_table(pairs, "audit_line")


def audit_offsets_from_tool_calls(audit: pd.DataFrame, tool_calls: pd.DataFrame) -> pd.DataFrame:
    """Per-host offset from audit decisions paired with the tool call that caused them.

    Pairs on (session_id, audit target = tool call target_resource); ``tool_calls``
    needs session_id, target_resource and a parsed UTC ``event_ts``. Covers every host
    (process_spawn / file_write / file_read / socket_connect all have a causing call).
    """
    a = audit.assign(local=parse_audit_local(audit.log_time), audit_line=audit.source_line)
    t = tool_calls.dropna(subset=["event_ts"])[["session_id", "target_resource", "event_ts"]]
    pairs = a.merge(t, left_on=["session_id", "target"], right_on=["session_id", "target_resource"])
    pairs["offset_s"] = (pairs.local - pairs.event_ts.dt.tz_localize(None)).dt.total_seconds()
    return _offset_table(pairs, "audit_line")


def lateness_seconds(ingest_ts: pd.Series, event_ts: pd.Series) -> pd.Series:
    """Lateness = ingest_time - event_time, in seconds (float, NaN if either is missing)."""
    return (ingest_ts - event_ts).dt.total_seconds()


def is_late(lateness_s: pd.Series, threshold_s: float = LATE_THRESHOLD_S) -> pd.Series:
    return (lateness_s > threshold_s).fillna(False)
