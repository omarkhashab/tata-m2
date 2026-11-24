"""Milestone 2 batch pipeline: contracts -> warehouse -> analytical storage.

    python -m kestrel_m2.build                       # everything, loads PostgreSQL
    python -m kestrel_m2.build --check-idempotency   # ... then loads a second time and compares
    python -m kestrel_m2.build --no-db               # contracts + model + Parquet only

Stages (each logs its timing):

1. **catalogue** every source file (path, format, size, SHA-256, rows, ingestion time)
2. **Pydantic gate** at the file readers: structure first, then every record
3. **M1 cleaning** (``kestrel_m1.pipeline.build``), then agreement of the gate with M1
4. **Pandera, curated zone**: M1's stage tables (quarantine or stop)
5. **DuckDB** flattens the policies (asserted equal to M1's flatten)
6. **star model** built from the curated tables
7. **Pandera, warehouse zone** on every star table (quarantine or stop)
8. **load** PostgreSQL idempotently + aggregates; optional run-twice check
9. Parquet mirror for Spark + the partitioned **lake** and CSV benchmark

Exit codes: 2 = structural contract break at a reader, 3 = table-level Pandera failure
between zones, 4 = the gate disagrees with M1, 5 = the run-twice check found a change.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd

from . import config


def log(msg: str, t0: float) -> None:
    print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Kestrel M2 batch pipeline")
    p.add_argument("--raw-dir", type=Path, default=None, help="raw pack directory (default M1/data/raw)")
    p.add_argument("--no-db", action="store_true", help="skip the PostgreSQL load")
    p.add_argument("--check-idempotency", action="store_true", help="load twice and compare counts + checksums")
    p.add_argument("--skip-storage", action="store_true", help="skip the partitioned lake and benchmark")
    args = p.parse_args(argv)

    raw_dir = Path(args.raw_dir or config.RAW_DIR).resolve()
    os.environ["KESTREL_RAW_DIR"] = str(raw_dir)
    from kestrel_m1 import pipeline, readers                 # noqa: E402  (after the env var is set)
    readers.RAW_DIR = raw_dir

    from . import catalog, gate, policies_duckdb, schemas, star, storage, warehouse

    t0 = time.time()
    out = config.OUT_DIR
    (out / "contracts").mkdir(parents=True, exist_ok=True)
    (out / "warehouse").mkdir(parents=True, exist_ok=True)

    # 1 catalogue ------------------------------------------------------------------
    cat = catalog.build_catalog(raw_dir)
    cat.drop(columns=["ingested_at"]).to_csv(out / "warehouse" / "source_file_catalog.csv", index=False)
    log(f"catalogue: {len(cat)} files, {cat.row_count.sum():,} records", t0)

    # 2 Pydantic gate ----------------------------------------------------------------
    try:
        verdicts = pd.concat([gate.validate_file(f, raw_dir) for f in gate.REFERENCE_SOURCES + gate.EVENT_SOURCES],
                             ignore_index=True)
    except gate.ContractError as e:
        print(f"CONTRACT BROKEN at the file reader: {e}", file=sys.stderr, flush=True)
        return 2
    log(f"pydantic gate: {len(verdicts):,} records validated, "
        f"{verdicts.contract_reason.notna().sum():,} refused by a record-level contract", t0)

    # 3 M1 cleaning + agreement --------------------------------------------------------
    tables = pipeline.build(t0)
    agree = gate.agreement(verdicts, tables["fact_rejected_record"])
    gsum = gate.summarise(agree)
    gsum.to_csv(out / "contracts" / "reader_gate_summary.csv", index=False)
    bad = agree[~agree.consistent]
    bad.to_csv(out / "contracts" / "reader_gate_disagreements.csv", index=False)
    if len(bad):
        print(f"the Pydantic gate disagrees with the M1 ladder on {len(bad)} records", file=sys.stderr)
        return 4
    log("gate agrees with the M1 reject ladder on every record", t0)

    # 4 Pandera: curated zone ------------------------------------------------------------
    try:
        curated, q_cur = schemas.enforce_curated(tables, catalog.id_by_file(cat))
    except schemas.ZoneContractError as e:
        print(f"ZONE CONTRACT BROKEN (raw -> curated): {e}", file=sys.stderr, flush=True)
        return 3
    log(f"pandera curated zone: {len(q_cur)} rows quarantined", t0)

    # 5 DuckDB policies -------------------------------------------------------------------
    pol = policies_duckdb.flatten(raw_dir / "sandbox_policies.json")
    mismatch = policies_duckdb.check_against_m1(pol["dim_policy"], tables["dim_policy"])
    mismatch.to_csv(out / "warehouse" / "policy_flatten_duckdb_vs_m1.csv", index=False)
    if len(mismatch):
        print(f"DuckDB policy flatten differs from M1 on {len(mismatch)} fields", file=sys.stderr)
        return 3
    log(f"duckdb: {len(pol['dim_policy'])} policy versions, {len(pol['policy_allowed_tool'])} allowed tools, "
        f"{len(pol['policy_egress_rule'])} egress rules (equal to M1's flatten)", t0)

    # 6-7 star model + Pandera: warehouse zone ----------------------------------------------
    frames = star.build_star(curated, cat, pol)
    try:
        frames, q_wh = schemas.enforce_warehouse(frames)
    except schemas.ZoneContractError as e:
        print(f"ZONE CONTRACT BROKEN (curated -> warehouse): {e}", file=sys.stderr, flush=True)
        return 3
    quarantine = pd.concat([q for q in (q_cur, q_wh) if len(q)], ignore_index=True) if len(q_cur) + len(q_wh) else None
    if quarantine is not None:
        rej = pd.concat([frames["fact_rejected_record"], star.shape("fact_rejected_record", quarantine)],
                        ignore_index=True).drop_duplicates(["source_file", "source_line"])
        frames["fact_rejected_record"] = rej
    log(f"pandera warehouse zone: {len(q_wh)} rows quarantined; "
        f"fact_rejected_record = {len(frames['fact_rejected_record']):,} rows", t0)
    counts = pd.DataFrame([{"table": k, "rows": len(v)} for k, v in frames.items()])
    counts.to_csv(out / "warehouse" / "row_counts.csv", index=False)

    # 9a Parquet mirror for Spark (written before the load so Spark never waits on it)
    warehouse.write_parquet_mirror(frames, out / "warehouse" / "parquet")
    canonical = tables["canonical_activity"]
    if quarantine is not None:
        bad_keys = set(zip(quarantine.source_file, quarantine.source_line))
        canonical = canonical[[k not in bad_keys for k in zip(canonical.source_file, canonical.source_line)]]
    canonical.to_parquet(out / "warehouse" / "canonical_activity.parquet", index=False)
    log("parquet mirror written to output/warehouse/parquet", t0)

    # 8 load ---------------------------------------------------------------------------------
    if not args.no_db:
        stats = warehouse.load(frames, log=lambda m: log(m, t0))
        stats.to_csv(out / "warehouse" / "load_run1.csv", index=False)
        c1 = warehouse.checksums()
        c1.to_csv(out / "warehouse" / "checksums.csv", index=False)
        log(f"loaded schema {config.DW_SCHEMA}: {c1.rows.sum():,} rows in {len(c1)} tables", t0)
        if args.check_idempotency:
            stats2 = warehouse.load(frames, log=lambda m: None)
            stats2.to_csv(out / "warehouse" / "load_run2.csv", index=False)
            c2 = warehouse.checksums()
            chk = c1.merge(c2, on="table", suffixes=("_run1", "_run2"))
            chk["rows_equal"] = chk.rows_run1 == chk.rows_run2
            chk["checksum_equal"] = chk.md5_run1 == chk.md5_run2
            chk = chk.merge(stats2[["table", "inserted", "updated", "deleted"]].rename(
                columns=lambda c: c if c == "table" else f"run2_{c}"), on="table", how="left")
            chk.to_csv(out / "warehouse" / "idempotency_check.csv", index=False)
            ok = bool(chk.rows_equal.all() and chk.checksum_equal.all())
            log(f"run-twice check: {'PASSED' if ok else 'FAILED'} "
                f"({chk.checksum_equal.sum()}/{len(chk)} tables with identical rows and checksums)", t0)
            if not ok:
                return 5

    # 9b lake + benchmark -------------------------------------------------------------------------
    if not args.skip_storage:
        res = storage.run(canonical)
        log(f"lake: {res['layouts'].to_dict('records')[-1]['files']} files partitioned by event_date; "
            f"benchmark in output/storage", t0)
    log("done", t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
