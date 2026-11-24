"""The contract gate at the file readers: every batch record through its Pydantic model.

    python -m kestrel_m2.gate            # writes output/contracts/reader_gate_*.csv

For each source file the gate

1. checks the file's **structure** first (Parquet physical schema, CSV header, JSON
   envelope). A structural break cannot be quarantined record by record, so it raises
   ``ContractError`` and the pipeline stops with a non-zero exit;
2. validates **every record** with its model (``contracts.MODELS``) and gives each
   refused record exactly one enum reason;
3. compares those verdicts with M1's reject ladder (``kestrel_m1``), which also applies
   the cross-record reasons (duplicates, unknown ids, orphans). The two must agree on
   every record: a record-local reason found by the contract must be the reason M1
   gave, unless M1 gave an earlier, cross-record reason. Disagreements are written out
   and fail the run.

The records the gate refuses are quarantined in ``fact_rejected_record`` (zone ``raw``)
by the warehouse load; the gate itself never drops anything.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from kestrel_m1 import readers
from kestrel_m1.rejects import REASONS

from . import config, contracts

CROSS_RECORD = {"DUPLICATE_EVENT_ID", "UNKNOWN_AGENT_ID", "UNKNOWN_TENANT_ID", "ORPHAN_EVENT", "LATE_BEYOND_WATERMARK"}

CSV_HEADERS = {
    "tenants.csv": ["tenant_id", "tenant_name", "industry", "plan_tier", "region", "onboarded_at", "is_active",
                    "security_contact"],
    "agents.csv": ["agent_id", "release_version", "agent_name", "agent_type", "model_id", "tenant_id",
                   "sandbox_profile_id", "owner_team", "autonomy_level", "max_tool_calls_per_session", "launch_date",
                   "is_deprecated"],
    "tools.csv": ["tool_id", "tool_version", "tool_name", "tool_category", "risk_tier", "requires_approval",
                  "owner_team"],
    "model_pricing.csv": ["model_id", "model_family", "input_price_per_1k", "output_price_per_1k", "currency",
                          "effective_from", "effective_to"],
    "network_flows.csv": ["flow_id", "session_id", "sandbox_instance_id", "flow_time", "src_ip", "dst_host", "dst_ip",
                          "dst_port", "protocol", "bytes_sent", "bytes_received", "verdict", "geo_country"],
    "incident_labels.csv": ["label_id", "session_id", "label", "attack_stage", "technique_ids", "labelled_by",
                            "labelled_at", "notes"],
}


class ContractError(RuntimeError):
    """A structural contract break: the file cannot be validated record by record."""


@dataclass
class GateResult:
    verdicts: pd.DataFrame        # one row per record: source_file, source_line, record_id, reason, detail, repairs
    summary: pd.DataFrame         # per file: records, accepted, rejected per reason, repairs


# --------------------------------------------------------------------------- records

def _clean(v):
    """pandas / pyarrow nulls -> None so a model sees exactly what the file holds."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if v is pd.NA or v is pd.NaT:
        return None
    return v


def _csv_records(name: str, raw_dir: Path) -> list[tuple[int, dict]]:
    path = raw_dir / name
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        expected = CSV_HEADERS[name]
        if header != expected:
            raise ContractError(f"{name}: header {header} != contract {expected}")
        out = []
        for i, row in enumerate(reader, start=2):        # header is line 1 (M1 readers convention)
            if len(row) != len(header):
                out.append((i, {"__unparseable__": ",".join(row)}))
                continue
            out.append((i, {k: (v if v != "" else None) for k, v in zip(header, row)}))
        return out


def check_parquet_schema(path: Path) -> None:
    actual = pq.read_schema(path)
    problems = []
    for col, typ in readers.EXPECTED_ACTIVITY_SCHEMA.items():
        if col not in actual.names:
            problems.append(f"missing column {col}")
        elif not actual.field(col).type.equals(typ):
            problems.append(f"{col}: {actual.field(col).type} != contract {typ}")
    if problems:
        raise ContractError(f"{path.name}: physical schema breaks the contract: " + "; ".join(problems))


def _activity_records(raw_dir: Path) -> list[tuple[int, dict]]:
    path = raw_dir / "agent_activity.parquet"
    check_parquet_schema(path)
    rows = pq.read_table(path).to_pylist()
    return list(enumerate(rows, start=1))


def _json_line_numbers(path: Path, key: str, n: int) -> list[int]:
    lines = [i for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1) if f'"{key}":' in line]
    return lines if len(lines) == n else list(range(1, n + 1))


def _policy_records(raw_dir: Path) -> list[tuple[int, dict]]:
    path = raw_dir / "sandbox_policies.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(doc, list):
        raise ContractError("sandbox_policies.json: expected a JSON array of policy versions")
    return list(zip(_json_line_numbers(path, "sandbox_profile_id", len(doc)), doc))


def _siem_records(raw_dir: Path) -> list[tuple[int, dict]]:
    path = raw_dir / "siem_alerts.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    env = contracts.check(contracts.SiemExport, doc)
    if not env.ok:
        raise ContractError(f"siem_alerts.json: envelope breaks the contract: {env.detail}")
    alerts = doc["alerts"]
    return list(zip(_json_line_numbers(path, "alert_id", len(alerts)), alerts))


def _audit_records(raw_dir: Path) -> list[tuple[int, dict]]:
    with open(raw_dir / "sandbox_audit.log", "rb") as fh:
        log = readers.parse_audit_lines(fh)
    out = [(int(r["source_line"]), {k: r[k] for k in readers.AUDIT_LINE_RE.groupindex})
           for r in log.parsed.to_dict("records")]
    out += [(int(r["source_line"]), {"__unparseable__": r["raw_line"]}) for r in log.unparsed.to_dict("records")]
    return sorted(out, key=lambda x: x[0])


ID_FIELD = {"agent_activity.parquet": "event_id", "network_flows.csv": "flow_id", "siem_alerts.json": "alert_id",
            "tenants.csv": "tenant_id", "agents.csv": "agent_id", "tools.csv": "tool_id", "model_pricing.csv": "model_id",
            "sandbox_policies.json": "sandbox_profile_id", "incident_labels.csv": "label_id",
            "sandbox_audit.log": None}


def records_of(name: str, raw_dir: Path) -> list[tuple[int, dict]]:
    if name == "agent_activity.parquet":
        return _activity_records(raw_dir)
    if name == "sandbox_policies.json":
        return _policy_records(raw_dir)
    if name == "siem_alerts.json":
        return _siem_records(raw_dir)
    if name == "sandbox_audit.log":
        return _audit_records(raw_dir)
    return _csv_records(name, raw_dir)


def validate_file(name: str, raw_dir: Path | None = None) -> pd.DataFrame:
    """One verdict row per record of ``name`` (structural breaks raise ContractError)."""
    raw_dir = Path(raw_dir or config.RAW_DIR)
    model = contracts.MODELS[name]
    id_field = ID_FIELD[name]
    rows = []
    for line, rec in records_of(name, raw_dir):
        if "__unparseable__" in rec:
            v = contracts.Verdict(reason="UNPARSEABLE_RECORD", detail="does not match the file's record layout")
        elif model is contracts.ActivityEvent:
            v = contracts.check_activity({k: _clean(x) for k, x in rec.items()})
        else:
            v = contracts.check(model, {k: _clean(x) for k, x in rec.items()} if isinstance(rec, dict) else rec)
        rows.append({"source_file": name, "source_line": line,
                     "record_id": rec.get(id_field) if id_field and isinstance(rec, dict) else None,
                     "contract_reason": v.reason, "contract_detail": v.detail, "repairs": "|".join(v.repairs)})
    return pd.DataFrame(rows, columns=["source_file", "source_line", "record_id", "contract_reason",
                                       "contract_detail", "repairs"])


# ----------------------------------------------------------------- agreement with M1

def agreement(verdicts: pd.DataFrame, m1_rejects: pd.DataFrame) -> pd.DataFrame:
    """Join contract verdicts with M1's reasons and classify each record.

    ``consistent`` is True when both accept, both give the same reason, or M1 gave an
    earlier cross-record reason that the single-record contract cannot see.
    """
    m1 = m1_rejects[["source_file", "source_line", "reason"]].rename(columns={"reason": "m1_reason"})
    v = verdicts.merge(m1, on=["source_file", "source_line"], how="left")
    rank = {r: i for i, r in enumerate(REASONS)}

    def ok(row) -> bool:
        p, m = row.contract_reason, row.m1_reason
        p = None if pd.isna(p) else p
        m = None if pd.isna(m) else m
        if p == m:
            return True
        if m in CROSS_RECORD and (p is None or rank[p] > rank[m]):
            return True
        return False

    v["consistent"] = v.apply(ok, axis=1) if len(v) else pd.Series(dtype=bool)
    return v


EVENT_SOURCES = ["agent_activity.parquet", "network_flows.csv", "sandbox_audit.log", "siem_alerts.json"]
REFERENCE_SOURCES = ["tenants.csv", "agents.csv", "tools.csv", "model_pricing.csv", "sandbox_policies.json"]


def run_gate(m1_rejects: pd.DataFrame, raw_dir: Path | None = None,
             files: list[str] | None = None) -> GateResult:
    """Validate every batch file and check agreement with M1 (labels are not read here)."""
    files = files or REFERENCE_SOURCES + EVENT_SOURCES
    verdicts = pd.concat([validate_file(f, raw_dir) for f in files], ignore_index=True)
    v = agreement(verdicts, m1_rejects)
    return GateResult(v, summarise(v))


def summarise(v: pd.DataFrame) -> pd.DataFrame:
    """Per file: records, accepted by the contract, repaired, agreeing with M1, refused per reason."""
    s = v.assign(accepted=v.contract_reason.isna(), repaired=v.repairs.fillna("") != "")
    summary = s.groupby("source_file", sort=False).agg(records=("source_line", "size"),
                                                       contract_accepted=("accepted", "sum"),
                                                       records_repaired=("repaired", "sum"),
                                                       agrees_with_m1=("consistent", "sum"))
    by_reason = s.dropna(subset=["contract_reason"]).pivot_table(index="source_file", columns="contract_reason",
                                                                values="source_line", aggfunc="size", fill_value=0)
    summary = summary.join(by_reason).fillna(0)
    ordered = [c for c in summary.columns if c not in REASONS] + [r for r in REASONS if r in summary.columns]
    return summary[ordered].astype("int64").reset_index()


def main() -> None:
    from kestrel_m1 import pipeline
    out = config.OUT_DIR / "contracts"
    out.mkdir(parents=True, exist_ok=True)
    tables = pipeline.build()
    res = run_gate(tables["fact_rejected_record"])
    res.summary.to_csv(out / "reader_gate_summary.csv", index=False)
    bad = res.verdicts[~res.verdicts.consistent]
    bad.to_csv(out / "reader_gate_disagreements.csv", index=False)
    print(res.summary.to_string(index=False))
    print(f"disagreements with M1: {len(bad)}")
    if len(bad):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
