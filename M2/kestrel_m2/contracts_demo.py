"""Show the contracts failing on deliberately broken input.

    python -m kestrel_m2.contracts_demo          # writes output/contracts/broken_input_demo.md

1. **Record level (Pydantic)**: a real activity row is broken one way at a time; each
   broken copy gets exactly one enum reason.
2. **Row level between zones (Pandera)**: real warehouse rows are broken (duplicate key,
   negative latency, unknown tenant, orphan session, impossible time, null status); the
   broken rows are quarantined with one reason each and the clean rows pass.
3. **The pipeline failing**: the real build (``python -m kestrel_m2.build``) is run on a
   copy of the raw pack with (a) ``agent_activity.parquet`` missing a column and with a
   wrong type, and (b) a stale ``network_flows.csv`` that stops on 15 February. Both
   runs stop with a non-zero exit code and say why. They write to a temporary output
   directory, never over the real outputs.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import config, contracts, schemas, star


def record_level() -> pd.DataFrame:
    rows = pq.read_table(config.RAW_DIR / "agent_activity.parquet").slice(0, 5000).to_pylist()
    tool = next(r for r in rows if r["event_type"] == "tool.call" and r["status"] == "success")
    cases = {
        "valid tool call (as read)": tool,
        "status 'succeeded' (repairable synonym)": dict(tool, status="succeeded"),
        "event_time '2026-02-30T10:00:00Z' (impossible date)": dict(tool, event_time="2026-02-30T10:00:00Z"),
        "event_time 'not-a-time'": dict(tool, event_time="not-a-time"),
        "event_type 'llm.thought'": dict(tool, event_type="llm.thought"),
        "status null": dict(tool, status=None),
        "tool_id key removed": {k: v for k, v in tool.items() if k != "tool_id"},
        "retry_number = -1": dict(tool, retry_number=-1),
        "latency_ms = 'fast' (not an int)": dict(tool, latency_ms="fast"),
        "status 'fine' (outside the enum)": dict(tool, status="fine"),
        "retry_number = -1 AND status null (two faults)": dict(tool, retry_number=-1, status=None),
        "truncated JSON text (not an object)": json.dumps(tool, default=str)[:200],
    }
    out = []
    for name, rec in cases.items():
        v = contracts.check_activity(rec)
        out.append({"broken input": name, "reason": v.reason or "ACCEPTED", "detail": v.detail or "",
                    "repairs": "|".join(v.repairs)})
    return pd.DataFrame(out)


def row_level() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    p = config.OUT_DIR / "warehouse" / "parquet"
    parents = {n: pd.read_parquet(p / f"{n}.parquet") for n in
               ["dim_source_file", "dim_date", "dim_time", "dim_tenant", "dim_agent", "dim_tool", "dim_policy",
                "fact_session"]}
    tc = pd.read_parquet(p / "fact_tool_call.parquet").sample(1000, random_state=1).reset_index(drop=True)
    broken = tc.copy()
    broken.loc[1, "event_id"] = broken.loc[0, "event_id"]                       # duplicate natural key
    broken.loc[2, "latency_ms"] = -5                                            # range
    broken.loc[3, "tenant_id"] = "tn_777"                                       # referential
    broken.loc[4, "session_id"] = "ss_9999999"                                  # orphan
    broken.loc[5, "event_time_utc"] = pd.Timestamp("2027-06-01", tz="UTC")      # outside the pack window
    broken.loc[6, "status"] = pd.NA                                             # required field
    broken.loc[7, "retry_number"] = -1
    schema = schemas.warehouse_schema("fact_tool_call", parents)
    clean, q = schemas.enforce(broken, schema, zone="warehouse", table="fact_tool_call", key=["event_id"])
    try:
        stale = tc[tc.event_time_utc < "2026-02-15"]
        schemas.enforce(stale, schema, zone="warehouse", table="fact_tool_call", key=["event_id"])
        table_level = "(no error)"
    except schemas.ZoneContractError as e:
        table_level = str(e)
    summary = pd.DataFrame([{"rows in": len(broken), "rows passed": len(clean), "rows quarantined": len(q)}])
    return summary, q[["record_id", "reason", "reason_detail"]], table_level


def run_build(raw_dir: Path, out_dir: Path) -> tuple[int, str]:
    env = dict(os.environ, KESTREL_RAW_DIR=str(raw_dir), KESTREL_M2_OUT=str(out_dir))
    proc = subprocess.run([sys.executable, "-m", "kestrel_m2.build", "--no-db", "--skip-storage",
                           "--raw-dir", str(raw_dir)], cwd=config.M2_DIR, env=env, capture_output=True, text=True)
    tail = "\n".join((proc.stdout.strip().splitlines() + proc.stderr.strip().splitlines())[-14:])
    return proc.returncode, tail


def pipeline_level() -> list[dict]:
    runs = []
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        # (a) structural break in the Parquet file
        raw = tmp / "raw_a"
        shutil.copytree(config.RAW_DIR, raw)
        t = pq.read_table(raw / "agent_activity.parquet")
        t = t.drop_columns(["ingest_time"])
        t = t.set_column(t.schema.get_field_index("sequence_number"), "sequence_number",
                         t.column("sequence_number").cast(pa.string()))
        pq.write_table(t, raw / "agent_activity.parquet", compression="zstd")
        code, tail = run_build(raw, tmp / "out_a")
        runs.append({"case": "agent_activity.parquet without ingest_time, sequence_number stored as text",
                     "exit_code": code, "output": tail})
        shutil.rmtree(raw)
        # (b) a stale extract: the flow file stops on 2026-02-15
        raw = tmp / "raw_b"
        shutil.copytree(config.RAW_DIR, raw)
        lines = (raw / "network_flows.csv").read_text(encoding="utf-8").splitlines(keepends=True)
        header, body = lines[0], lines[1:]
        col = header.rstrip("\n").split(",").index("flow_time")
        keep = [ln for ln in body if ln.split(",")[col] < "2026-02-15"]
        (raw / "network_flows.csv").write_text(header + "".join(keep), encoding="utf-8", newline="")
        code, tail = run_build(raw, tmp / "out_b")
        runs.append({"case": f"network_flows.csv truncated to {len(keep):,} of {len(body):,} rows (before 15 Feb)",
                     "exit_code": code, "output": tail})
    return runs


def main() -> None:
    out = config.OUT_DIR / "contracts"
    out.mkdir(parents=True, exist_ok=True)
    rec = record_level()
    summary, quarantined, table_level = row_level()
    runs = pipeline_level()
    md = ["# Contracts on deliberately broken input", "",
          "Generated by `python -m kestrel_m2.contracts_demo`.", "",
          "## 1. Record level: the Pydantic activity contract", "",
          "A real `tool.call` row from `agent_activity.parquet`, broken one way at a time. Each broken copy gets "
          "exactly one enum reason, the first in the closed order when two apply.", "",
          rec.to_markdown(index=False), "",
          "## 2. Row level between zones: Pandera, `fact_tool_call`", "",
          "1,000 real rows (random sample), seven of them broken. The broken rows are quarantined with one reason "
          "each and the rest pass.", "",
          summary.to_markdown(index=False), "", quarantined.to_markdown(index=False), "",
          "A table-level failure (here, a stale extract that ends on 15 February) cannot be pinned on a row, so it "
          "raises and stops the pipeline:", "", "```", table_level, "```", "",
          "## 3. The pipeline failing", "",
          "The real build, run on a copy of the raw pack with one file broken:", ""]
    for r in runs:
        md += [f"### {r['case']}", "", f"exit code **{r['exit_code']}**", "", "```", r["output"], "```", ""]
    (out / "broken_input_demo.md").write_text("\n".join(md), encoding="utf-8")
    print("\n".join(md))
    if not all(r["exit_code"] != 0 for r in runs):
        raise SystemExit("a broken input did not stop the pipeline")


if __name__ == "__main__":
    main()
