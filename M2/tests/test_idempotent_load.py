"""The run-twice test: load the same data twice; row counts and checksums must not change.

Runs against the compose PostgreSQL in a separate schema (``kestrel_dw_test``) on a
deterministic sample of the warehouse (every dimension, 1,500 sessions with all their
facts), so it takes seconds and never touches ``kestrel_dw``. The full-size version of
the same check runs inside the pipeline (``python -m kestrel_m2.build
--check-idempotency`` -> ``output/warehouse/idempotency_check.csv``).
"""

import pandas as pd
import pytest

from kestrel_m2 import config, star, warehouse

PARQUET = config.OUT_DIR / "warehouse" / "parquet"
SCHEMA = "kestrel_dw_test"


def _db_ok() -> bool:
    try:
        warehouse.connect().close()
        return True
    except Exception:                                   # noqa: BLE001
        return False


pytestmark = [pytest.mark.db,
              pytest.mark.skipif(not (PARQUET / "fact_session.parquet").exists(), reason="no Parquet mirror"),
              pytest.mark.skipif(not _db_ok(), reason="PostgreSQL not reachable")]


def sample_frames(n_sessions: int = 1500, seed: int = 42) -> dict[str, pd.DataFrame]:
    f = {t.name: pd.read_parquet(PARQUET / f"{t.name}.parquet") for t in star.TABLES}
    keep = set(f["fact_session"].session_id.sample(n_sessions, random_state=seed))
    for name in ("fact_session", "fact_prompt_turn", "fact_tool_call", "fact_network_flow", "fact_sandbox_decision"):
        f[name] = f[name][f[name].session_id.isin(keep)]
    a = f["fact_siem_alert"]
    flows = set(f["fact_network_flow"].flow_id)
    f["fact_siem_alert"] = a[(a.session_id.isin(keep) | a.session_id.isna())
                             & (a.matched_flow_id.isna() | a.matched_flow_id.isin(flows))]
    f["fact_rejected_record"] = f["fact_rejected_record"].head(2000)
    return {k: v.reset_index(drop=True) for k, v in f.items()}


@pytest.fixture(scope="module")
def clean_schema():
    conn = warehouse.connect()
    with conn, conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.close()
    yield SCHEMA
    conn = warehouse.connect()
    with conn, conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.close()


def test_load_twice_changes_nothing(clean_schema):
    frames = sample_frames()
    first = warehouse.load(frames, clean_schema, log=lambda m: None)
    c1 = warehouse.checksums(clean_schema)
    second = warehouse.load(frames, clean_schema, log=lambda m: None)
    c2 = warehouse.checksums(clean_schema)

    assert (first.inserted == first.staged).all()                       # first load inserts everything
    assert second[["inserted", "updated", "deleted"]].sum().sum() == 0   # second load touches nothing
    pd.testing.assert_frame_equal(c1, c2)                                # same row counts and md5 per table
    assert c1.set_index("table").loc["fact_session", "rows"] == 1500


def test_a_changed_row_is_updated_and_a_missing_row_pruned(clean_schema):
    frames = sample_frames()
    warehouse.load(frames, clean_schema, log=lambda m: None)
    changed = dict(frames)
    tc = frames["fact_tool_call"].copy()
    tc.loc[tc.index[0], "latency_ms"] = 123456
    changed["fact_tool_call"] = tc.iloc[:-1]                             # one row changed, the last one gone
    stats = warehouse.load(changed, clean_schema, log=lambda m: None).set_index("table")
    assert stats.loc["fact_tool_call", ["inserted", "updated", "deleted"]].tolist() == [0, 1, 1]
    assert stats.drop(index="fact_tool_call")[["inserted", "updated", "deleted"]].sum().sum() == 0
