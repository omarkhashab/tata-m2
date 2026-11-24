"""Lab 5 analytical storage: the curated activity table as partitioned Parquet.

    python -m kestrel_m2.storage          # (re)builds output/lake and output/storage/*

The curated ``canonical_activity`` table (M1, 1.15 M rows) is written

* as one **CSV** file (the baseline),
* as one **Parquet** file (zstd),
* as **hive-partitioned Parquet**, partition key ``event_date`` (the UTC day of
  ``event_time_utc``): ``output/lake/canonical_activity/event_date=2026-03-14/part-0.parquet``.
  Rows are sorted by ``(event_time_utc, activity_id)`` inside each file, so row-group
  min/max statistics also prune time ranges within a day. File names are deterministic
  and the write replaces matching partitions, so re-running is idempotent.

It then benchmarks the same DuckDB queries over the three layouts (median of several
runs), records sizes and file counts, studies what other partition keys would do to
the file count, and saves DuckDB's ``EXPLAIN ANALYZE`` to show partition pruning.
"""

from __future__ import annotations

import shutil
import statistics
import tempfile
import time
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from . import config

LAKE = config.OUT_DIR / "lake"
REPORT = config.OUT_DIR / "storage"
PARTITION_KEY = "event_date"
RUNS = 5

QUERIES = {
    "Q1 one day (partition pruning)":
        "SELECT activity_type, count(*) AS n, count(*) FILTER (WHERE is_policy_violation) AS violations "
        "FROM {src} WHERE event_date = DATE '2026-03-14' GROUP BY 1 ORDER BY 1",
    "Q2 full scan aggregate":
        "SELECT tenant_id, activity_type, count(*) AS n, "
        "round(avg(CASE WHEN is_policy_violation THEN 1.0 ELSE 0.0 END), 6) AS violation_rate "
        "FROM {src} WHERE is_policy_violation IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2",
    "Q3 narrow projection (2 columns)":
        "SELECT tenant_id, round(sum(cost_usd), 2) AS cost_usd FROM {src} "
        "WHERE activity_type = 'prompt_turn' GROUP BY 1 ORDER BY 1",
    "Q4 one week range":
        "SELECT event_date, count(*) AS n FROM {src} "
        "WHERE event_date BETWEEN DATE '2026-03-01' AND DATE '2026-03-07' GROUP BY 1 ORDER BY 1",
    "Q5 point lookup (one session)":
        "SELECT activity_id, event_time_utc, activity_type, status FROM {src} "
        "WHERE session_id = 'ss_003274' ORDER BY event_time_utc, activity_id",
}


def _with_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["event_time_utc", "activity_id"], kind="stable").reset_index(drop=True)
    df[PARTITION_KEY] = df.event_time_utc.dt.strftime("%Y-%m-%d")
    return df


def _dir_stats(path: Path) -> tuple[int, int]:
    files = [p for p in path.rglob("*") if p.is_file()] if path.is_dir() else [path]
    return len(files), sum(p.stat().st_size for p in files)


def write_partitioned(df: pd.DataFrame, root: Path, keys: list[str]) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    part = ds.partitioning(pa.schema([table.schema.field(k) for k in keys]), flavor="hive")
    ds.write_dataset(table, root, format="parquet", partitioning=part, basename_template="part-{i}.parquet",
                     existing_data_behavior="delete_matching",
                     file_options=ds.ParquetFileFormat().make_write_options(compression="zstd"),
                     max_rows_per_group=131072)


def build_layouts(canonical: pd.DataFrame) -> pd.DataFrame:
    """Write CSV, single Parquet and the partitioned lake; return size / file count / write time."""
    df = _with_date(canonical)
    LAKE.mkdir(parents=True, exist_ok=True)
    layouts = []

    csv = LAKE / "csv" / "canonical_activity.csv"
    csv.parent.mkdir(exist_ok=True)
    t = time.perf_counter()
    df.to_csv(csv, index=False)
    layouts.append(("csv", csv, time.perf_counter() - t))

    single = LAKE / "parquet_single" / "canonical_activity.parquet"
    single.parent.mkdir(exist_ok=True)
    t = time.perf_counter()
    df.to_parquet(single, index=False, compression="zstd")
    layouts.append(("parquet_single", single, time.perf_counter() - t))

    lake = LAKE / "canonical_activity"
    if lake.exists():
        shutil.rmtree(lake)
    t = time.perf_counter()
    write_partitioned(df, lake, [PARTITION_KEY])
    layouts.append(("parquet_partitioned_by_event_date", lake, time.perf_counter() - t))

    rows = []
    for name, path, secs in layouts:
        n, size = _dir_stats(path)
        rows.append({"layout": name, "path": path.relative_to(config.M2_DIR).as_posix(), "files": n,
                     "size_mb": round(size / 2**20, 2), "write_s": round(secs, 2), "rows": len(df)})
    return pd.DataFrame(rows)


def partition_key_study(canonical: pd.DataFrame) -> pd.DataFrame:
    """What each candidate partition key does to the file count and file sizes."""
    df = _with_date(canonical)
    df["event_month"] = df.event_time_utc.dt.strftime("%Y-%m")
    candidates = {"none (single file)": [], "event_month": ["event_month"], "event_date": ["event_date"],
                  "activity_type": ["activity_type"], "tenant_id": ["tenant_id"],
                  "event_date + activity_type": ["event_date", "activity_type"],
                  "event_date + tenant_id": ["event_date", "tenant_id"]}
    rows = []
    with tempfile.TemporaryDirectory() as tmp:
        for label, keys in candidates.items():
            root = Path(tmp) / label.replace(" ", "_").replace("+", "")
            sub = df.drop(columns=[c for c in ("event_month", "event_date") if c not in keys])
            if keys:
                write_partitioned(sub.assign(tenant_id=sub.tenant_id.fillna("__none__")) if "tenant_id" in keys
                                  else sub, root, keys)
            else:
                root.mkdir(parents=True)
                sub.to_parquet(root / "part-0.parquet", index=False, compression="zstd")
            files = [p for p in root.rglob("*.parquet")]
            sizes = pd.Series([p.stat().st_size for p in files]) / 2**20
            rows.append({"partition_key": label, "files": len(files), "total_mb": round(sizes.sum(), 2),
                         "min_file_mb": round(sizes.min(), 3), "median_file_mb": round(sizes.median(), 3),
                         "max_file_mb": round(sizes.max(), 3),
                         "max_over_median": round(sizes.max() / sizes.median(), 1)})
    return pd.DataFrame(rows)


def _sources(root: Path = LAKE) -> dict[str, str]:
    csv = (root / "csv" / "canonical_activity.csv").as_posix()
    single = (root / "parquet_single" / "canonical_activity.parquet").as_posix()
    lake = (root / "canonical_activity" / "**" / "*.parquet").as_posix()
    return {
        "csv": f"(SELECT * REPLACE (CAST(event_date AS DATE) AS event_date) FROM read_csv('{csv}', header = true, "
               f"types = {{'event_date': 'VARCHAR'}}))",
        "parquet_single": f"(SELECT * REPLACE (CAST(event_date AS DATE) AS event_date) FROM read_parquet('{single}'))",
        "parquet_partitioned": f"read_parquet('{lake}', hive_partitioning = true)",
    }


def benchmark(root: Path = LAKE, filesystem: str = "output directory") -> tuple[pd.DataFrame, dict]:
    """Median wall time of each query on each layout (warm OS cache)."""
    rows, results = [], {}
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    try:
        for qname, sql in QUERIES.items():
            outs = {}
            for layout, src in _sources(root).items():
                q = sql.format(src=src)
                con.execute(q).fetchall()                       # warm-up
                times = []
                for _ in range(RUNS):
                    t = time.perf_counter()
                    res = con.execute(q).df()
                    times.append(time.perf_counter() - t)
                outs[layout] = res
                rows.append({"filesystem": filesystem, "query": qname, "layout": layout,
                             "median_ms": round(1000 * statistics.median(times), 1),
                             "min_ms": round(1000 * min(times), 1), "rows_returned": len(res)})
            first = next(iter(outs.values()))
            for layout, res in outs.items():                    # every layout must give the same answer
                pd.testing.assert_frame_equal(first.reset_index(drop=True), res.reset_index(drop=True),
                                              check_dtype=False)
            results[qname] = first
    finally:
        con.close()
    bench = pd.DataFrame(rows)
    base = bench[bench.layout == "csv"].set_index("query").median_ms
    bench["speedup_vs_csv"] = (bench["query"].map(base) / bench.median_ms).round(1)
    return bench, results


def _buffered_copy(src, dst) -> None:
    """Plain buffered copy. shutil's default uses os.sendfile, which fails with ENOMEM on a
    Docker Desktop bind mount when the container is under memory pressure."""
    with open(src, "rb") as fi, open(dst, "wb") as fo:
        shutil.copyfileobj(fi, fo, 8 << 20)


def benchmark_both() -> tuple[pd.DataFrame, dict]:
    """Benchmark on a copy on the process's own local disk (the storage layouts themselves) and
    on the output directory as written. In docker compose the output directory is a bind mount
    from the host; on Docker Desktop (Windows/macOS) every file open crosses the VM boundary,
    which penalises layouts with many files. Both are reported."""
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp)
        for sub in ("csv", "parquet_single", "canonical_activity"):
            shutil.copytree(LAKE / sub, local / sub, copy_function=_buffered_copy)
        local_bench, results = benchmark(local, "local disk")
    out_bench, _ = benchmark(LAKE, "output directory (bind mount in docker)")
    return pd.concat([local_bench, out_bench], ignore_index=True), results


def explain_pruning() -> str:
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")
        con.execute("PRAGMA enable_profiling = 'no_output'")
        out = []
        for layout in ("parquet_partitioned", "parquet_single"):
            q = QUERIES["Q1 one day (partition pruning)"].format(src=_sources()[layout])
            plan = con.execute("EXPLAIN ANALYZE " + q).fetchall()
            out.append(f"-- {layout}\n" + "\n".join(r[1] for r in plan))
        return "\n\n".join(out)
    finally:
        con.close()


def run(canonical: pd.DataFrame) -> dict[str, pd.DataFrame]:
    REPORT.mkdir(parents=True, exist_ok=True)
    layouts = build_layouts(canonical)
    study = partition_key_study(canonical)
    bench, results = benchmark_both()
    plan = explain_pruning()
    layouts.to_csv(REPORT / "layouts.csv", index=False)
    study.to_csv(REPORT / "partition_key_file_counts.csv", index=False)
    bench.to_csv(REPORT / "benchmark.csv", index=False)
    md = ["# DuckDB queries over the curated activity lake", "",
          "Generated by `python -m kestrel_m2.storage`. Every query was run on the CSV, the single Parquet file and "
          "the partitioned lake; the three answers are asserted identical. Timings are in `benchmark.csv`.", ""]
    for qname, sql in QUERIES.items():
        md += [f"## {qname}", "", "```sql", sql.format(src=_sources()["parquet_partitioned"]), "```", "",
               results[qname].to_markdown(index=False) if len(results[qname]) <= 40 else
               results[qname].head(40).to_markdown(index=False), ""]
    md += ["## EXPLAIN ANALYZE of Q1 (partition pruning)", "", "```", plan, "```", ""]
    text = "\n".join(md)
    for prefix in (config.M2_DIR.as_posix() + "/", str(config.M2_DIR) + "\\"):   # keep the report portable
        text = text.replace(prefix, "")
    (REPORT / "duckdb_queries.md").write_text(text, encoding="utf-8")
    return {"layouts": layouts, "study": study, "benchmark": bench}


def main() -> None:
    curated = config.OUT_DIR / "warehouse" / "canonical_activity.parquet"
    if curated.exists():
        canonical = pd.read_parquet(curated)
    else:
        from kestrel_m1 import pipeline
        canonical = pipeline.build()["canonical_activity"]
    out = run(canonical)
    for k, v in out.items():
        print(f"== {k}\n{v.to_string(index=False)}")


if __name__ == "__main__":
    main()
