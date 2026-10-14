import pandas as pd

from kestrel_m1 import reference as rf


def ts(s):
    return pd.Timestamp(s, tz="UTC")


def test_duplicate_tenant_is_merged_into_canonical(ref):
    assert ref.tenant_alias == {"tn_004": "tn_003"}
    assert set(ref.tenants.tenant_id) == {"tn_001", "tn_002", "tn_003", "tn_005", "tn_006"}
    bad = ref.tenants.set_index("tenant_id").loc["tn_005"]
    assert pd.isna(bad.region) and bad.region_raw == "eu-central-9"
    assert "INVALID_ENUM_REGION" in bad.quality_flags


def test_agent_versions_are_deduplicated_and_windowed(ref):
    a = ref.agents
    assert len(a) == 21 and a.agent_sk.is_unique
    assert not (a.release_version.str.startswith("v")).any()
    ag7 = a[a.agent_id == "ag_007"].set_index("release_version")
    assert ag7.loc["2.3.1", "autonomy_level"] == "semi_autonomous"         # canonical row won
    assert ag7.loc["2.2.0", "valid_to_utc"] == ag7.loc["2.3.1", "valid_from_utc"] == ts("2026-02-10")
    assert a.groupby("agent_id").is_current.sum().eq(1).all()
    assert "NO_POLICY_PROFILE" in a.set_index("agent_id").loc["ag_019", "quality_flags"]


def test_every_dropped_reference_row_is_logged(ref):
    er = ref.entity_resolution
    assert {"tenant", "agent_version", "tool_version"} <= set(er.entity)
    assert er[er.entity != "tool_name"].source_line.notna().all()


def test_tool_aliases_and_dedup(ref):
    assert len(ref.tools) == 17
    assert ref.tool_alias["http_get"] == ref.tool_alias["webfetch"] == "tl_web_fetch"
    ids, rewritten = rf.resolve_tool_id(pd.Series(["http_get", "tl_sql_query", None]), ref.tool_alias,
                                        set(ref.tools.tool_id))
    assert list(ids[:2]) == ["tl_web_fetch", "tl_sql_query"] and pd.isna(ids[2])
    assert list(rewritten) == [True, False, False]


def test_price_effective_at_turn_time(ref):
    turns = pd.DataFrame({"model_id": ["mdl_kst_pro_1", "mdl_kst_pro_1", "mdl_kst_nano_1", "mdl_kst_nano_1"],
                          "t": [ts("2026-02-28 23:59:59"), ts("2026-03-01"), ts("2026-01-31 12:00"), ts("2026-02-10")]})
    out = rf.attach_price(turns, "model_id", "t", ref.pricing)
    assert list(out.input_price_per_1k[:2]) == [0.003, 0.0025]               # price change on 1 March
    assert out.input_price_per_1k[2:].isna().all()                          # nano: before first price / no price


def test_policy_overlap_newer_version_wins(ref):
    rows = pd.DataFrame({"profile": ["sp_research_std"] * 3 + ["sp_ops_elevated", "sp_missing_profile"],
                         "t": [ts("2026-01-20"), ts("2026-01-28"), ts("2026-03-01"), ts("2026-02-20"), ts("2026-02-20")]})
    out = rf.attach_policy(rows, "profile", "t", ref.policies)
    assert list(out.policy_version_applied[:3]) == [2, 3, 3]                 # inside the overlap -> v3
    assert out.policy_version_applied[3] == 4 and not out.has_network_block[3]
    assert pd.isna(out.policy_version_applied[4])


def test_agent_sk_resolves_prefixed_versions_and_checks_window(ref):
    rows = pd.DataFrame({"agent_id": ["ag_007", "ag_007", "ag_999"], "release_version": ["v2.3.1", "2.2.0", "9.9.9"],
                         "t": [ts("2026-03-01"), ts("2026-03-01"), ts("2026-03-01")]})
    out = rf.attach_agent_sk(rows, ref.agents, "t")
    assert out.agent_sk[0] == ref.agents.set_index(["agent_id", "release_version"]).loc[("ag_007", "2.3.1"), "agent_sk"]
    assert out.agent_version_outside_window[1]                               # 2.2.0 retired on 10 Feb
    assert pd.isna(out.agent_sk[2])
