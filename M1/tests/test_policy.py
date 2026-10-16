import pandas as pd
import pytest

from kestrel_m1 import policy


def ts(s):
    return pd.Timestamp(s, tz="UTC")


def test_wildcards_are_patterns_not_strings():
    allow = ["api.kestrel.ai", "*.public-data.example"]
    assert policy.host_allowed("census.public-data.example", allow)
    assert policy.host_allowed("CENSUS.Public-Data.example", allow)                 # DNS is case-insensitive
    assert not policy.host_allowed("public-data.example", allow)                    # apex is not covered
    assert not policy.host_allowed("a.b.public-data.example", allow)                # one label only
    assert not policy.host_allowed("public-data.example.evil", allow)
    assert not policy.host_allowed("evilpublic-data.example", allow)
    assert not policy.host_allowed(None, allow) and not policy.host_allowed("api.kestrel.ai", [])


def activity_rows(rows):
    base = dict(event_type="tool.call", sandbox_profile_id="sp_ops_elevated", status="success", tool_id="tl_web_fetch",
                injection_indicators=None, input_tokens=None, output_tokens=None, model_id=None, quality_flags="")
    df = pd.DataFrame([{**base, **r} for r in rows])
    for c in ["input_tokens", "output_tokens"]:
        df[c] = df[c].astype("Int64")
    return df


def test_tool_policy_follows_the_version_in_effect(ref):
    df = activity_rows([dict(event_time_utc=ts("2026-02-14 23:59")),                 # v3: web_fetch not allowed
                        dict(event_time_utc=ts("2026-02-15 00:00")),                 # v4: allowed
                        dict(event_time_utc=ts("2026-02-20"), sandbox_profile_id="sp_missing_profile"),
                        dict(event_time_utc=ts("2026-02-20"), event_type="session.end", tool_id=None)])
    out = policy.enrich_activity(df, ref)
    assert list(out.policy_version_applied[:2]) == [3, 4]
    assert out.is_policy_violation[0] and not out.is_policy_violation[1]
    assert out.risk_indicators[0] == "tool_not_allowed"
    assert pd.isna(out.is_policy_violation[2]) and "NO_POLICY_IN_EFFECT" in out.quality_flags[2]
    assert pd.isna(out.is_policy_violation[3])                                        # no rule for lifecycle rows


def test_cost_at_turn_time_and_missing_token_policy(ref):
    turn = dict(event_type="llm.turn", tool_id=None, status="ok", model_id="mdl_kst_pro_1", input_tokens=1000,
                output_tokens=100, injection_indicators="role_confusion|exfil_request")
    df = activity_rows([{**turn, "event_time_utc": ts("2026-02-28 12:00")},
                        {**turn, "event_time_utc": ts("2026-03-01 12:00")},
                        {**turn, "event_time_utc": ts("2026-03-01 12:00"), "output_tokens": None},
                        {**turn, "event_time_utc": ts("2026-03-01 12:00"), "model_id": "mdl_kst_nano_1"}])
    out = policy.enrich_activity(df, ref)
    assert out.cost_usd[0] == pytest.approx(0.003 + 0.0015)                           # old pro_1 price
    assert out.cost_usd[1] == pytest.approx(0.0025 + 0.0012)                          # price change on 1 March
    assert pd.isna(out.cost_usd[2]) and pd.isna(out.cost_usd[3])                      # null-excluded, never 0
    assert "PRICE_MISSING" in out.quality_flags[3]
    assert out.risk_indicators[0] == "role_confusion|exfil_request"


def flow_frame(rows):
    base = dict(sandbox_profile_id="sp_research_std", flow_time_utc=ts("2026-03-01"), dst_host="census.public-data.example",
                dst_port=443, verdict="allowed", quality_flags="")
    df = pd.DataFrame([{**base, **r} for r in rows])
    df["dst_port"] = df.dst_port.astype("Int64")
    return df


def test_egress_rules(ref):
    out = policy.enrich_flows(flow_frame([
        {},                                                                           # allowlisted via wildcard
        dict(dst_host="drop.exfil-host.example"),
        dict(dst_host=None, dst_port=445, verdict="blocked"),
        dict(sandbox_profile_id="sp_ops_elevated", dst_host="api.kestrel.ai"),        # v4: no network block
    ]), ref)
    assert list(out.is_policy_violation) == [False, True, True, True]
    assert list(out.violation_rule) == ["", "egress_not_allowlisted", "port_denied|egress_host_unknown",
                                        "no_egress_allowed"]
    assert out.risk_indicators[2] == "port_denied|egress_host_unknown|egress_blocked"


def test_sandbox_fail_open_is_still_a_violation(ref):
    base = dict(sandbox_profile_id="sp_ops_elevated", event_time_utc=ts("2026-03-01"), action="socket_connect",
                policy_version_reported=4, quality_flags="")
    df = pd.DataFrame([{**base, "decision": "ALLOW", "reason": "NO_NETWORK_POLICY", "target": "api.kestrel.ai:443"},
                       {**base, "decision": "DENY", "reason": "ESCALATION_FORBIDDEN", "action": "setuid", "target": "0"},
                       {**base, "decision": "ALLOW", "reason": "TOOL_ALLOWED", "action": "process_spawn",
                        "target": "/usr/bin/ls", "policy_version_reported": 3}])
    df["policy_version_reported"] = df.policy_version_reported.astype("Int64")
    out = policy.enrich_audit(df, ref)
    assert list(out.is_policy_violation) == [True, True, False]
    assert "allowed_despite_policy" in out.risk_indicators[0]
    assert out.risk_indicators[1] == "sandbox_denied|escalation_forbidden|escalation_attempt"
    assert "POLICY_VERSION_MISMATCH" in out.quality_flags[2]


def test_full_pack_checkpoint(ref, pack):
    a = policy.enrich_activity(pack.act.accepted, ref)
    f = policy.enrich_flows(pack.fl.accepted, ref)
    u = policy.enrich_audit(pack.au.accepted, ref)
    tc = a[a.event_type == "tool.call"]
    assert tc.is_policy_violation.sum() == 6_583 and tc.is_policy_violation.isna().sum() == 15_923   # ag_019
    assert (tc[tc.status == "denied"].is_policy_violation == True).all()            # every platform denial agrees
    assert f.is_policy_violation.sum() == 7_458
    assert u.is_policy_violation.sum() == 6_463 + 4_130                                # DENY + fail-open
    assert not u.quality_flags.str.contains("POLICY_VERSION_MISMATCH").any()           # window join == sandbox
    assert round(a.cost_usd.sum(), 2) == 4_055.11
