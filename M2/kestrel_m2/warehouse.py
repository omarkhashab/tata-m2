"""Lab 5: load the star schema into PostgreSQL, idempotently, and build the aggregates.

Load semantics (``load``), all inside **one transaction** so readers never see a half
refreshed warehouse:

1. every table is bulk-copied into a temporary staging table (``COPY``);
2. **upsert** on the declared natural key (the primary key of the spec)::

       INSERT INTO t SELECT * FROM stg
       ON CONFLICT (pk) DO UPDATE SET c = EXCLUDED.c, ..., loaded_at = now()
       WHERE (t.c, ...) IS DISTINCT FROM (EXCLUDED.c, ...)

   An identical row is not touched at all (its ``loaded_at`` stays), so re-running the
   same load changes nothing, not even timestamps;
3. **prune**: rows whose key is no longer produced are deleted, children before parents.
   ``fact_rejected_record`` is pruned only in its batch partition (``channel = 'batch'``)
   so the stream consumer's rejects survive a batch reload. The source-file catalogue is
   insert-once (``ON CONFLICT DO NOTHING``): it keeps the first ingestion time of every
   file content.

Aggregates use **delete-then-insert of the whole table** (the table is one partition),
computed by SQL from the facts (``sql/aggregates.sql``), with rates as ``numeric`` so the
result is bit-for-bit reproducible.

``checksums`` gives (row count, md5 over every row in key order) per table: the
run-twice test asserts both are unchanged after a second identical load.
"""

from __future__ import annotations

import argparse
import io
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

from . import config, star

SQL_DIR = Path(__file__).resolve().parent / "sql"
AGGREGATES = ["agent_daily_behaviour", "tenant_risk_metrics", "policy_violation_metrics", "data_quality_metrics"]


def connect():
    return psycopg2.connect(config.psycopg_dsn())


def _pg_array(v) -> str | None:
    """A list (or numpy array, as read back from Parquet) as a PostgreSQL array literal."""
    if not isinstance(v, (list, tuple, np.ndarray)):
        return None if v is None or pd.isna(v) else _pg_array([v])
    items = []
    for x in v:
        s = str(x).replace("\\", "\\\\").replace('"', '\\"')
        items.append(f'"{s}"')
    return "{" + ",".join(items) + "}"


def _csv(df: pd.DataFrame, spec: star.Table) -> io.StringIO:
    out = df.copy()
    for c in spec.cols:
        if c.type.endswith("[]"):
            out[c.name] = out[c.name].map(_pg_array)
        elif c.type == "date":
            out[c.name] = out[c.name].dt.strftime("%Y-%m-%d")
        elif c.type == "timestamptz":
            out[c.name] = out[c.name].map(lambda t: None if pd.isna(t) else t.isoformat())
    buf = io.StringIO()
    out.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    return buf


def create_schema(cur, schema: str) -> None:
    cur.execute(star.ddl(schema))
    cur.execute((SQL_DIR / "aggregates_ddl.sql").read_text().format(s=schema))


def _upsert(cur, schema: str, spec: star.Table, df: pd.DataFrame) -> dict:
    cols = spec.columns
    stg = f"stg_{spec.name}"
    cur.execute(f"CREATE TEMP TABLE {stg} (LIKE {schema}.{spec.name} INCLUDING DEFAULTS) ON COMMIT DROP")
    cur.copy_expert(f"COPY {stg} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", _csv(df, spec))
    collist = ", ".join(cols)
    conflict = ", ".join(spec.pk)
    non_key = [c for c in cols if c not in spec.pk]
    if spec.upsert == "nothing" or not non_key:          # insert-once, or a table that is all key
        action = "DO NOTHING"
    else:
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_key)
        old = ", ".join(f"t.{c}" for c in non_key)
        new = ", ".join(f"EXCLUDED.{c}" for c in non_key)
        action = f"DO UPDATE SET {sets}, loaded_at = now() WHERE ({old}) IS DISTINCT FROM ({new})"
    cur.execute(f"INSERT INTO {schema}.{spec.name} AS t ({collist}) SELECT {collist} FROM {stg} "
                f"ON CONFLICT ({conflict}) {action} RETURNING (xmax = 0) AS inserted")
    flags = [r[0] for r in cur.fetchall()]
    return {"table": spec.name, "staged": len(df), "inserted": sum(flags), "updated": len(flags) - sum(flags)}


def _prune(cur, schema: str, spec: star.Table) -> int:
    if spec.upsert == "nothing":
        return 0
    match = " AND ".join(f"s.{c} = t.{c}" for c in spec.pk)
    where = f"({spec.prune_where}) AND " if spec.prune_where else ""
    cur.execute(f"DELETE FROM {schema}.{spec.name} t WHERE {where}"
                f"NOT EXISTS (SELECT 1 FROM stg_{spec.name} s WHERE {match})")
    return cur.rowcount


def refresh_aggregates(cur, schema: str) -> None:
    """Delete-then-insert every aggregate table from the facts (one transaction)."""
    cur.execute((SQL_DIR / "aggregates.sql").read_text().format(s=schema))


def load(frames: dict[str, pd.DataFrame], schema: str | None = None, log=print) -> pd.DataFrame:
    """Idempotent load of every star table + aggregate refresh; returns per-table stats."""
    schema = schema or config.DW_SCHEMA
    t0 = time.time()
    conn = connect()
    try:
        with conn:
            with conn.cursor() as cur:
                create_schema(cur, schema)
                stats = []
                for spec in star.TABLES:
                    stats.append(_upsert(cur, schema, spec, frames[spec.name]))
                    log(f"upsert {schema}.{spec.name}: {stats[-1]}")
                for spec in reversed(star.TABLES):
                    n = _prune(cur, schema, spec)
                    next(s for s in stats if s["table"] == spec.name)["deleted"] = n
                refresh_aggregates(cur, schema)
                log("aggregates refreshed")
        return pd.DataFrame(stats)
    finally:
        conn.close()


def checksums(schema: str | None = None, tables: list[str] | None = None) -> pd.DataFrame:
    """(rows, md5 of all rows in primary-key order) per table, including loaded_at."""
    schema = schema or config.DW_SCHEMA
    tables = tables or [t.name for t in star.TABLES] + AGGREGATES
    conn = connect()
    rows = []
    try:
        with conn.cursor() as cur:
            for name in tables:
                cur.execute("""SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                               FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid
                                AND a.attnum = ANY(i.indkey)
                               WHERE i.indrelid = %s::regclass AND i.indisprimary""", (f"{schema}.{name}",))
                pk = cur.fetchone()[0]
                cur.execute(f"SELECT count(*), coalesce(md5(string_agg(md5(t::text), '' ORDER BY {pk})), '') "
                            f"FROM {schema}.{name} t")
                n, digest = cur.fetchone()
                rows.append({"table": name, "rows": n, "md5": digest})
    finally:
        conn.close()
    return pd.DataFrame(rows)


def write_parquet_mirror(frames: dict[str, pd.DataFrame], out_dir: Path) -> None:
    """The same star tables as Parquet, for Spark (identical rows to what was loaded)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in frames.items():
        df.to_parquet(out_dir / f"{name}.parquet", index=False)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="warehouse utilities")
    p.add_argument("--print-ddl", action="store_true", help="write sql/ddl.sql from the spec")
    p.add_argument("--refresh-aggregates", action="store_true")
    p.add_argument("--checksums", action="store_true")
    args = p.parse_args(argv)
    if args.print_ddl:
        text = star.ddl("kestrel_dw") + "\n" + (SQL_DIR / "aggregates_ddl.sql").read_text().format(s="kestrel_dw")
        (SQL_DIR / "ddl.sql").write_text("-- generated by: python -m kestrel_m2.warehouse --print-ddl\n" + text)
        (config.M2_DIR / "docs" / "warehouse_columns.md").write_text(star.columns_markdown(), encoding="utf-8")
        print(f"wrote {SQL_DIR / 'ddl.sql'} and docs/warehouse_columns.md")
    if args.refresh_aggregates:
        conn = connect()
        with conn, conn.cursor() as cur:
            refresh_aggregates(cur, config.DW_SCHEMA)
        conn.close()
        print("aggregates refreshed")
    if args.checksums:
        print(checksums().to_string(index=False))


if __name__ == "__main__":
    main()
