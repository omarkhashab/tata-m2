import pandas as pd

from kestrel_m1 import canonical, correlate, pipeline, policy


def test_lists_and_dates_are_made_sql_friendly():
    df = pd.DataFrame({"allowed_tools": [["a", "b"], []], "launch_date": [pd.Timestamp("2026-01-01").date()] * 2})
    out = pipeline._for_sql(df)
    assert list(out.allowed_tools) == ["a|b", ""]
    assert str(out.launch_date.dtype).startswith("datetime64")


def test_siem_rows_carry_their_correlated_session():
    alerts = pd.DataFrame({"alert_id": ["al_1", "al_2", "al_3"], "source_file": "siem_alerts.json",
                           "source_line": [4, 20, 36], "detected_at_utc": pd.Timestamp("2026-03-01", tz="UTC"),
                           "rule": "r", "destination_host": "10.0.0.9", "destination_port": 445,
                           "technique_id": "T1046"})
    corr = pd.DataFrame({"alert_id": ["al_1", "al_2", "al_3"], "match_status": ["matched", "ambiguous", "unmatched"],
                         "session_id": ["ss_1", "ss_1", None],
                         "session_resolution": ["single_flow", "single_session", "none"]})
    sessions = pd.DataFrame({"session_id": ["ss_1"], "tenant_id": ["tn_003"], "agent_sk": [8], "task_id": ["tk_1"]})
    out = canonical.from_siem(alerts, corr, sessions).set_index("activity_id")
    assert out.loc["siem:al_1", "tenant_id"] == "tn_003" and out.loc["siem:al_1", "agent_sk"] == 8
    assert out.loc["siem:al_2", "quality_flags"] == "CORRELATION_AMBIGUOUS_FLOW_SINGLE_SESSION"
    assert pd.isna(out.loc["siem:al_3", "tenant_id"]) and out.loc["siem:al_3", "quality_flags"] == "CORRELATION_UNMATCHED"
    assert out.loc["siem:al_1", "target_resource"] == "10.0.0.9:445"


def test_full_pack_canonical(ref, pack):
    corr = correlate.correlate(pack.si.accepted, pack.fl.accepted)
    canon = canonical.build_canonical(policy.enrich_activity(pack.act.accepted, ref),
                                      policy.enrich_flows(pack.fl.accepted, ref),
                                      policy.enrich_audit(pack.au.accepted, ref),
                                      pack.si.accepted, corr.alerts, pack.sess)
    received = 937_752 + 42_392 + 184_005 + 3_754
    rejected = sum(len(s.rejects) for s in (pack.act, pack.fl, pack.au, pack.si))
    assert len(canon) == 1_154_314 and len(canon) + rejected == received
    assert canon.activity_type.value_counts().to_dict() == {
        "prompt_turn": 407_462, "tool_call": 319_361, "lifecycle": 197_952, "sandbox_decision": 183_477,
        "network_flow": 42_308, "siem_alert": 3_754}
    assert canon[canon.activity_type != "siem_alert"].session_id.notna().all()
    assert canon[canon.activity_type != "siem_alert"].tenant_id.notna().all()
