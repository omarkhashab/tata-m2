"""Pandera zones, the file-reader gate's structural checks, the DuckDB policy flatten."""

import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from kestrel_m2 import config, gate, policies_duckdb, schemas, star

PARQUET = config.OUT_DIR / "warehouse" / "parquet"
needs_mirror = pytest.mark.skipif(not (PARQUET / "fact_tool_call.parquet").exists(),
                                  reason="run python -m kestrel_m2.build first")


@needs_mirror
def test_warehouse_schema_quarantines_rows_with_one_reason():
    parents = {n: pd.read_parquet(PARQUET / f"{n}.parquet") for n in
               ["dim_source_file", "dim_date", "dim_time", "dim_tenant", "dim_agent", "dim_tool", "dim_policy",
                "fact_session"]}
    tc = pd.read_parquet(PARQUET / "fact_tool_call.parquet").sample(500, random_state=3).reset_index(drop=True)
    schema = schemas.warehouse_schema("fact_tool_call", parents)
    clean, q = schemas.enforce(tc, schema, zone="warehouse", table="fact_tool_call", key=["event_id"])
    assert len(clean) == 500 and q.empty
    bad = tc.copy()
    bad.loc[1, "event_id"] = bad.loc[0, "event_id"]
    bad.loc[2, "tenant_id"] = "tn_777"
    bad.loc[3, "latency_ms"] = -1
    bad.loc[3, "session_id"] = "ss_nope"                                  # two faults: ORPHAN wins (earlier)
    clean, q = schemas.enforce(bad, schema, zone="warehouse", table="fact_tool_call", key=["event_id"])
    assert len(clean) == 497
    assert dict(zip(q.reason_detail.str.contains("ss_nope|session_id"), q.reason)) is not None
    assert sorted(q.reason) == ["DUPLICATE_EVENT_ID", "ORPHAN_EVENT", "UNKNOWN_TENANT_ID"]
    assert q.source_file.eq("agent_activity.parquet").all() and q.raw_payload.notna().all()


@needs_mirror
def test_table_level_failures_stop_the_pipeline():
    parents = {n: pd.read_parquet(PARQUET / f"{n}.parquet") for n in ["dim_tenant", "dim_agent", "dim_date"]}
    tc = pd.read_parquet(PARQUET / "fact_tool_call.parquet")
    schema = schemas.warehouse_schema("fact_tool_call", parents)
    with pytest.raises(schemas.ZoneContractError, match="FRESHNESS"):
        schemas.enforce(tc[tc.event_time_utc < "2026-03-01"], schema, zone="warehouse", table="t", key=["event_id"])
    with pytest.raises(schemas.ZoneContractError):
        schemas.enforce(tc.drop(columns=["status"]).head(100), schema, zone="warehouse", table="t", key=["event_id"])


def test_gate_stops_on_a_structural_break(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    t = pq.read_table(config.RAW_DIR / "agent_activity.parquet").slice(0, 100).drop_columns(["ingest_time"])
    pq.write_table(t, raw / "agent_activity.parquet")
    with pytest.raises(gate.ContractError, match="missing column ingest_time"):
        gate.validate_file("agent_activity.parquet", raw)
    shutil.copy(config.RAW_DIR / "tools.csv", raw / "tools.csv")
    text = (raw / "tools.csv").read_text().replace("risk_tier", "risk")
    (raw / "tools.csv").write_text(text)
    with pytest.raises(gate.ContractError, match="header"):
        gate.validate_file("tools.csv", raw)


def test_gate_reference_files_all_valid():
    v = pd.concat([gate.validate_file(f) for f in gate.REFERENCE_SOURCES])
    assert v.contract_reason.isna().all()
    assert set(v.repairs) == {"", "INVALID_ENUM_REGION", "VERSION_PREFIX_STRIPPED"}


def test_duckdb_flatten_equals_m1(ref):
    out = policies_duckdb.flatten()
    assert policies_duckdb.check_against_m1(out["dim_policy"], ref.policies).empty
    v4 = out["dim_policy"].query("sandbox_profile_id == 'sp_ops_elevated' and policy_version == 4").iloc[0]
    assert not v4.has_network_block and v4.egress_allowlist == [] and "NO_NETWORK_BLOCK_NO_EGRESS" in v4.quality_flags
    rx = out["policy_egress_rule"].query("is_wildcard").match_regex.iloc[0]
    import re
    assert re.fullmatch(rx, "census.public-data.example") and not re.fullmatch(rx, "a.b.public-data.example")


def test_ddl_renders_every_table_with_its_natural_key():
    ddl = star.ddl("x")
    for t in star.TABLES:
        assert f"CREATE TABLE IF NOT EXISTS x.{t.name}" in ddl
    assert "PRIMARY KEY (agent_id, release_version)" in ddl
    assert "PRIMARY KEY (sandbox_profile_id, policy_version)" in ddl
