import pandas as pd

from kestrel_m1 import activity, audit, correlate, flows, readers, siem


def ts(s):
    return pd.Timestamp(s, tz="UTC")


def sessions(*ids):
    return pd.DataFrame([{**dict.fromkeys(activity.SESSION_COLUMNS), "session_id": s, "tenant_id": "tn_003",
                          "agent_sk": 7, "sandbox_profile_id": "sp_research_std", "sandbox_instance_id": "sbx_0001"}
                         for s in ids])


# ----------------------------------------------------------------------- flows

def flow_rows(rows):
    cols = ["flow_id", "session_id", "sandbox_instance_id", "flow_time", "src_ip", "dst_host", "dst_ip", "dst_port",
            "protocol", "bytes_sent", "bytes_received", "verdict", "geo_country"]
    base = dict(sandbox_instance_id="sbx_0001", flow_time="2026-03-01T10:00:00.000Z", src_ip="10.20.1.1",
                dst_port="443", protocol="https", bytes_sent="100", bytes_received="200", verdict="allowed",
                geo_country="US")
    df = pd.DataFrame([{**base, **r} for r in rows], columns=cols)
    df.insert(0, "source_line", range(2, len(df) + 2))
    df.insert(0, "source_file", "network_flows.csv")
    return df


def test_flows_orphans_and_dst_host_fill():
    raw = flow_rows([
        dict(flow_id="nf_1", session_id="ss_1", dst_host="a.example", dst_ip="1.1.1.1"),
        dict(flow_id="nf_2", session_id="ss_1", dst_host=None, dst_ip="1.1.1.1"),     # fillable
        dict(flow_id="nf_3", session_id="ss_1", dst_host=None, dst_ip="9.9.9.9", dst_port="22", protocol="ssh"),
        dict(flow_id="nf_4", session_id="ss_404", dst_host="a.example", dst_ip="1.1.1.1"),
        dict(flow_id="nf_5", session_id="ss_1", dst_host="a.example", dst_ip="1.1.1.1", dst_port="70000"),
    ])
    res = flows.clean_flows(sessions("ss_1"), raw)
    assert dict(zip(res.rejects.record_id, res.rejects.reason)) == {"nf_4": "ORPHAN_EVENT",
                                                                    "nf_5": "INVALID_NUMERIC_RANGE"}
    a = res.accepted.set_index("flow_id")
    assert a.loc["nf_2", "dst_host"] == "a.example" and a.loc["nf_2", "quality_flags"] == "DST_HOST_FILLED"
    assert pd.isna(a.loc["nf_3", "dst_host"]) and a.loc["nf_3", "target_resource"] == "9.9.9.9:22"
    assert a.loc["nf_1", "tenant_id"] == "tn_003" and a.loc["nf_1", "agent_sk"] == 7


def test_ambiguous_ip_is_never_used_to_fill_a_host():
    df = flow_rows([dict(flow_id="nf_1", session_id="ss_1", dst_host="a.example", dst_ip="1.1.1.1"),
                    dict(flow_id="nf_2", session_id="ss_1", dst_host="b.example", dst_ip="1.1.1.1")])
    assert flows.ip_host_map(df).empty


# ----------------------------------------------------------------------- audit

LINE = ("{t} {host} kestrel-sandbox[1234]: session={sid} instance=sbx_0001 decision=ALLOW action=process_spawn "
        "target={target} reason=TOOL_ALLOWED policy_version={pv}")


def audit_log(*lines):
    return readers.parse_audit_lines([l.encode() + b"\n" for l in lines])


def test_audit_clock_duplicates_and_unparseable():
    host_a_slow = LINE.format(t="Mar 14 11:21:34", host="sbx-a", sid="ss_1", target="/usr/bin/ls", pv="3")
    lines = [
        host_a_slow,                                                                         # 1: anchor
        LINE.format(t="Mar 14 11:30:00", host="sbx-a", sid="ss_1", target="/usr/bin/cat", pv="-"),
        LINE.format(t="Mar 14 11:30:00", host="sbx-b", sid="ss_1", target="/usr/bin/cat", pv="3"),
        host_a_slow,                                                                         # 4: duplicate
        "Mar 14 11:30:01 sbx-a kestrel-sandbox[1234]: session=ss_1 inst",                   # 5: truncated
        LINE.format(t="Mar 14 11:30:02", host="sbx-a", sid="ss_404", target="/usr/bin/ls", pv="3"),
    ]
    calls = pd.DataFrame({"event_type": ["tool.call"], "session_id": ["ss_1"], "target_resource": ["/usr/bin/ls"],
                          "event_time_utc": [ts("2026-03-14 09:22:04")]})
    res = audit.clean_audit(sessions("ss_1"), calls, audit_log(*lines))
    reasons = dict(zip(res.rejects.source_line, res.rejects.reason))
    assert reasons == {4: "DUPLICATE_EVENT_ID", 5: "UNPARSEABLE_RECORD", 6: "ORPHAN_EVENT"}
    assert res.rejects.set_index("source_line").loc[5, "raw_payload"] == lines[4]          # raw line kept
    a = res.accepted.set_index("source_line")
    assert a.loc[1, "host_offset_s"] == 7170                                               # host is 30 s slow
    assert a.loc[1, "event_time_utc"] == ts("2026-03-14 09:22:04")
    assert a.loc[2, "event_time_utc"] == ts("2026-03-14 09:30:30")                         # same correction
    assert pd.isna(a.loc[2, "policy_version_reported"]) and "POLICY_VERSION_NOT_LOGGED" in a.loc[2, "quality_flags"]
    assert a.loc[3, "event_time_utc"] == ts("2026-03-14 09:30:00")                         # unseen host: UTC+2
    assert "CLOCK_OFFSET_FALLBACK_UTC_PLUS_2" in a.loc[3, "quality_flags"]
    assert res.summary.rows.sum() == len(lines)                                            # nothing dropped


# ------------------------------------------------------------------------ siem

def test_siem_offsets_converted_and_offset_required():
    alerts = pd.DataFrame({
        "source_file": "siem_alerts.json", "source_line": [4, 20, 36],
        "alert_id": ["al_1", "al_2", "al_3"], "source_system": ["corp-siem", "cloud-ids", "corp-siem"],
        "severity": ["high", "low", "high"],
        "detected_at": ["2026-03-14T13:22:41+02:00", "2026-03-14T06:22:41-05:00", "2026-03-14T11:22:41"],
        "source_ip": "10.20.9.113", "rule": "r", "confidence": 0.8, "destination_host": "10.20.4.19",
        "destination_port": 445, "technique_id": "T1021.002", "technique_name": "SMB"})
    res = siem.clean_siem(readers.SiemExport({}, alerts))
    assert list(res.rejects.reason) == ["MALFORMED_TIMESTAMP"]                              # naive time refused
    assert (res.accepted.detected_at_utc == ts("2026-03-14 11:22:41")).all()


# ------------------------------------------------------------------- correlate

def test_correlation_statuses():
    alerts = pd.DataFrame({"alert_id": ["al_m", "al_s", "al_x", "al_u", "al_far"],
                           "source_ip": ["10.0.0.1", "10.0.0.2", "10.0.0.3", "8.8.8.8", "10.0.0.1"],
                           "destination_host": "10.9.9.9", "destination_port": 445,
                           "detected_at_utc": [ts("2026-03-01 10:00:20")] * 4 + [ts("2026-03-01 11:00:00")]})
    flows_ = pd.DataFrame({"flow_id": ["nf_1", "nf_2", "nf_3", "nf_4", "nf_5"],
                           "session_id": ["ss_1", "ss_2", "ss_2", "ss_3", "ss_4"],
                           "src_ip": ["10.0.0.1", "10.0.0.2", "10.0.0.2", "10.0.0.3", "10.0.0.3"],
                           "dst_ip": "10.9.9.9", "dst_port": pd.array([445] * 5, dtype="Int64"),
                           "flow_time_utc": [ts("2026-03-01 10:00:00")] * 5})
    c = correlate.correlate(alerts, flows_, n_s=60).alerts.set_index("alert_id")
    assert c.loc["al_m", "match_status"] == "matched" and c.loc["al_m", "flow_id"] == "nf_1"
    assert c.loc["al_m", "delta_s"] == 20
    assert c.loc["al_s", "match_status"] == "ambiguous" and c.loc["al_s", "session_id"] == "ss_2"
    assert c.loc["al_s", "session_resolution"] == "single_session" and pd.isna(c.loc["al_s", "flow_id"])
    assert c.loc["al_x", "match_status"] == "ambiguous" and pd.isna(c.loc["al_x", "session_id"])
    assert c.loc["al_u", "match_status"] == "unmatched" and pd.isna(c.loc["al_u", "nearest_key_match_s"])
    assert c.loc["al_far", "match_status"] == "unmatched" and c.loc["al_far", "nearest_key_match_s"] == 3600


# ------------------------------------------------------------- full pack

def test_full_pack_checkpoint(pack):
    sess, fl, au, si = pack.sess, pack.fl, pack.au, pack.si
    assert len(sess) == 100_000
    outcome = lambda r: dict(zip(r.summary.outcome, r.summary.rows))
    assert outcome(fl) == {"ACCEPTED": 42_308, "ORPHAN_EVENT": 84}
    assert outcome(au) == {"ACCEPTED": 183_477, "DUPLICATE_EVENT_ID": 4, "UNPARSEABLE_RECORD": 524}
    assert outcome(si) == {"ACCEPTED": 3_754}
    assert len(au.host_offsets) == 42 and au.host_offsets.skew_vs_utc_plus_2_s.abs().max() < 100
    c = correlate.correlate(si.accepted, fl.accepted)
    assert dict(zip(c.counts().match_status, c.counts().alerts)) == {"matched": 2_409, "ambiguous": 27,
                                                                     "unmatched": 1_318}
    assert c.alerts.session_id.notna().sum() == 2_435
