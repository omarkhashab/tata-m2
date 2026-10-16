"""Run the whole M1 batch pipeline: raw files -> curated Parquet -> PostgreSQL.

    python -m kestrel_m1.pipeline                  # Parquet + load into Postgres
    python -m kestrel_m1.pipeline --no-db          # Parquet only

Both also write the M1 metrics (``kestrel_m1.metrics``) to ``output/metrics/``.

Database settings come from ``KESTREL_DB_URL`` (default: the compose Postgres on
localhost) and ``KESTREL_DB_SCHEMA`` (default ``kestrel``). Every table is replaced
on each run, so the pipeline is idempotent.
"""

from __future__ import annotations

import argparse
import io
import os
import time
from pathlib import Path

import pandas as pd

from . import activity, audit, canonical, correlate, flows, metrics, policy, siem
from .reference import load_reference
from .rejects import REJECT_COLUMNS, check_rejects

DEFAULT_OUT = Path(__file__).resolve().parents[1] / "output" / "curated"
DEFAULT_DB_URL = "postgresql+psycopg2://kestrel:kestrel@localhost:5432/kestrel"

# primary keys and lookup indexes created after the bulk load
KEYS = {
    "canonical_activity": (["activity_id"], [["session_id"], ["event_time_utc"], ["activity_type"]]),
    "fact_rejected_record": (["reject_id"], [["source_file", "source_line"], ["reason"]]),
    "stg_agent_activity": (["event_id"], [["session_id"]]),
    "stg_network_flows": (["flow_id"], [["session_id"], ["src_ip", "dst_ip", "dst_port"]]),
    "stg_sandbox_audit": (["audit_id"], [["session_id"]]),
    "stg_siem_alerts": (["alert_id"], []),
    "siem_correlation": (["alert_id"], [["session_id"]]),
    "session": (["session_id"], []),
    "dim_tenant": (["tenant_id"], []),
    "dim_agent": (["agent_sk"], [["agent_id", "release_version"]]),
    "dim_tool": (["tool_id", "tool_version"], []),
    "dim_policy": (["sandbox_profile_id", "policy_version"], []),
    "dim_model_price": (["model_id", "effective_from_utc"], []),
}


def _log(msg: str, t0: float) -> None:
    print(f"[{time.time() - t0:6.1f}s] {msg}", flush=True)


def build(t0: float | None = None) -> dict[str, pd.DataFrame]:
    """Run every stage and return all output tables by name."""
    t0 = time.time() if t0 is None else t0
    ref = load_reference()
    _log("reference data cleaned", t0)

    act = activity.clean_activity(ref=ref)
    sessions = activity.session_table(act.accepted)
    _log(f"activity: {len(act.accepted):,} accepted, {len(act.rejects):,} rejected", t0)
    fl = flows.clean_flows(sessions)
    au = audit.clean_audit(sessions, act.accepted)
    si = siem.clean_siem()
    _log(f"flows {len(fl.accepted):,} / audit {len(au.accepted):,} / siem {len(si.accepted):,} accepted", t0)

    corr = correlate.correlate(si.accepted, fl.accepted)
    sens = correlate.sensitivity(si.accepted, fl.accepted)
    _log("correlation: " + ", ".join(f"{r.match_status} {r.alerts:,}" for r in corr.counts().itertuples()), t0)

    a = policy.enrich_activity(act.accepted, ref)
    f = policy.enrich_flows(fl.accepted, ref)
    u = policy.enrich_audit(au.accepted, ref)
    canon = canonical.build_canonical(a, f, u, si.accepted, corr.alerts, sessions)
    _log(f"canonical_activity: {len(canon):,} rows", t0)

    stages = {"agent_activity.parquet": act, "network_flows.csv": fl, "sandbox_audit.log": au,
              "siem_alerts.json": si}
    rejects = pd.concat([s.rejects for s in stages.values()], ignore_index=True)
    rejects = rejects.sort_values(["source_file", "source_line"], ignore_index=True)
    check_rejects(rejects)
    rejects.insert(0, "reject_id", range(1, len(rejects) + 1))
    summary = pd.concat([s.summary.assign(source_file=name) for name, s in stages.items()], ignore_index=True)
    summary = summary[["source_file", "outcome", "rows", "share_pct"]]
    _log(f"fact_rejected_record: {len(rejects):,} rows", t0)

    # every received record is either canonical or rejected, per source
    canon_by_file = canon.source_file.value_counts()
    for name, s in stages.items():
        assert canon_by_file.get(name, 0) == len(s.accepted), name

    return {
        "canonical_activity": canon,
        "fact_rejected_record": rejects[["reject_id"] + REJECT_COLUMNS],
        "stage_summary": summary,
        "stg_agent_activity": a,
        "stg_network_flows": f,
        "stg_sandbox_audit": u,
        "stg_siem_alerts": si.accepted,
        "siem_correlation": corr.alerts,
        "siem_correlation_candidates": corr.candidates,
        "correlation_sensitivity": sens,
        "session": sessions,
        "audit_host_offsets": au.host_offsets,
        "dim_tenant": ref.tenants,
        "dim_agent": ref.agents,
        "dim_tool": ref.tools,
        "dim_policy": ref.policies,
        "dim_model_price": ref.pricing,
        "entity_resolution": ref.entity_resolution,
    }


def write_parquet(tables: dict[str, pd.DataFrame], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_parquet(out_dir / f"{name}.parquet", index=False)


def _for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """Lists (policy allowlists, scopes) become pipe-separated text; dates become timestamps."""
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == object:
            sample = df[c].dropna()
            if len(sample) and isinstance(sample.iloc[0], (list, tuple)):
                df[c] = df[c].map(lambda v: "|".join(map(str, v)) if isinstance(v, (list, tuple)) else v)
            elif len(sample) and hasattr(sample.iloc[0], "isoformat") and not isinstance(sample.iloc[0], str):
                df[c] = pd.to_datetime(df[c])
    return df


def load_postgres(tables: dict[str, pd.DataFrame], db_url: str, schema: str, t0: float) -> None:
    """Replace every table in ``schema`` with a bulk COPY, then add keys and indexes."""
    from sqlalchemy import create_engine          # only needed when loading

    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    for name, df in tables.items():
        df = _for_sql(df)
        df.head(0).to_sql(name, engine, schema=schema, if_exists="replace", index=False)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            cols = ", ".join(f'"{c}"' for c in df.columns)
            cur.copy_expert(f'COPY "{schema}"."{name}" ({cols}) FROM STDIN WITH (FORMAT csv, NULL \'\\N\')', buf)
            pk, indexes = KEYS.get(name, ([], []))
            if pk:
                cur.execute(f'ALTER TABLE "{schema}"."{name}" ADD PRIMARY KEY ({", ".join(pk)})')
            for i, cols_ in enumerate(indexes):
                cur.execute(f'CREATE INDEX "{name}_ix{i}" ON "{schema}"."{name}" ({", ".join(cols_)})')
            raw.commit()
        finally:
            raw.close()
        _log(f"loaded {schema}.{name}: {len(df):,} rows", t0)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Kestrel M1 batch pipeline")
    p.add_argument("--out", type=Path, default=Path(os.environ.get("KESTREL_OUT_DIR", DEFAULT_OUT)))
    p.add_argument("--no-db", action="store_true", help="write Parquet only, skip the Postgres load")
    p.add_argument("--db-url", default=os.environ.get("KESTREL_DB_URL", DEFAULT_DB_URL))
    p.add_argument("--schema", default=os.environ.get("KESTREL_DB_SCHEMA", "kestrel"))
    args = p.parse_args(argv)

    t0 = time.time()
    tables = build(t0)
    write_parquet(tables, args.out)
    _log(f"wrote {len(tables)} Parquet tables to {args.out}", t0)
    values = metrics.write_outputs(tables, args.out.parent / "metrics")
    _log(f"wrote {len(values)} metric values to {args.out.parent / 'metrics'}", t0)
    if not args.no_db:
        load_postgres(tables, args.db_url, args.schema, t0)
    print(tables["stage_summary"].to_string(index=False))
    _log("done", t0)


if __name__ == "__main__":
    main()
