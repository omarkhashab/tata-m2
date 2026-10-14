"""Stage 1 readers: load every Kestrel source file in its own format.

Every reader returns the data *as exported* (no cleaning, no type coercion beyond
what the format itself carries) plus two provenance columns that must survive to
the warehouse:

* ``source_file``: the file name inside the raw data directory
* ``source_line``: the 1-based physical line in that file where the record starts
  (for Parquet, which has no lines, the 1-based row number)

Cleaning lives in later modules; these functions only read.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

RAW_DIR = Path(os.environ.get("KESTREL_RAW_DIR", Path(__file__).resolve().parents[1] / "data" / "raw"))

FILES = {
    "tenants": ("tenants.csv", "csv"),
    "agents": ("agents.csv", "csv"),
    "tools": ("tools.csv", "csv"),
    "model_pricing": ("model_pricing.csv", "csv"),
    "sandbox_policies": ("sandbox_policies.json", "nested_json"),
    "agent_activity": ("agent_activity.parquet", "parquet"),
    "network_flows": ("network_flows.csv", "csv"),
    "sandbox_audit": ("sandbox_audit.log", "syslog"),
    "siem_alerts": ("siem_alerts.json", "nested_json"),
    # Milestone 2 only: listed for the inventory, never read by the M1 pipeline.
    "stream_events": ("stream_events.json", "json_array"),
    "incident_labels": ("incident_labels.csv", "csv"),
}

# Dataset Description 3.5: counts are int64, the three timestamps are raw strings.
EXPECTED_ACTIVITY_SCHEMA = {
    "event_id": pa.string(),
    "event_time": pa.string(),
    "ingest_time": pa.string(),
    "sequence_number": pa.int64(),
    "event_type": pa.string(),
    "session_id": pa.string(),
    "tenant_id": pa.string(),
    "agent_id": pa.string(),
    "release_version": pa.string(),
    "sandbox_instance_id": pa.string(),
    "status": pa.string(),
    "latency_ms": pa.int64(),
    "turn_index": pa.int64(),
    "model_id": pa.string(),
    "content_source": pa.string(),
    "stated_goal": pa.string(),
    "injection_indicators": pa.string(),
    "input_tokens": pa.int64(),
    "output_tokens": pa.int64(),
    "finish_reason": pa.string(),
    "tool_id": pa.string(),
    "tool_version": pa.string(),
    "finished_at": pa.string(),
    "retry_number": pa.int64(),
    "error_class": pa.string(),
    "target_resource": pa.string(),
    "bytes_out": pa.int64(),
    "task_id": pa.string(),
    "task_category": pa.string(),
    "expected_tool_categories": pa.string(),
}

# Dataset Description 3.7. Anchored at both ends so a truncated line never half-matches.
AUDIT_LINE_RE = re.compile(
    r"^(?P<log_time>[A-Z][a-z]{2} [ \d]\d \d{2}:\d{2}:\d{2}) "
    r"(?P<host>\S+) "
    r"(?P<process>[\w.\-]+)\[(?P<pid>\d+)\]: "
    r"session=(?P<session_id>\S+) "
    r"instance=(?P<sandbox_instance_id>\S+) "
    r"decision=(?P<decision>\S+) "
    r"action=(?P<action>\S+) "
    r"target=(?P<target>\S+) "
    r"reason=(?P<reason>\S+) "
    r"policy_version=(?P<policy_version>\S+)$"
)


def path_of(name: str) -> Path:
    return RAW_DIR / FILES[name][0]


def _with_provenance(df: pd.DataFrame, file_name: str, lines) -> pd.DataFrame:
    df = df.copy()
    df.insert(0, "source_line", lines)
    df.insert(0, "source_file", file_name)
    return df


# --------------------------------------------------------------------------- CSV

def read_csv(name: str) -> pd.DataFrame:
    """Read a pack CSV with every column as text.

    ``dtype=str`` stops pandas from silently coercing ids or versions (``1.10`` to
    ``1.1``), and ``keep_default_na=False`` with ``na_values=[""]`` honours the pack's
    null convention exactly: only an empty field is null, not the string ``"NA"``.
    """
    file_name = FILES[name][0]
    df = pd.read_csv(path_of(name), dtype=str, keep_default_na=False, na_values=[""])
    # header is line 1, so data row i (0-based) sits on line i + 2 (no field in the
    # pack contains an embedded newline; file_inventory() verifies this)
    return _with_provenance(df, file_name, range(2, len(df) + 2))


def read_tenants() -> pd.DataFrame:
    return read_csv("tenants")


def read_agents() -> pd.DataFrame:
    return read_csv("agents")


def read_tools() -> pd.DataFrame:
    return read_csv("tools")


def read_model_pricing() -> pd.DataFrame:
    return read_csv("model_pricing")


def read_network_flows() -> pd.DataFrame:
    return read_csv("network_flows")


# ------------------------------------------------------------------------ Parquet

def activity_schema_check() -> pd.DataFrame:
    """Compare the Parquet file's physical schema with the documented contract."""
    actual = pq.read_schema(path_of("agent_activity"))
    rows = []
    for col in sorted(set(EXPECTED_ACTIVITY_SCHEMA) | set(actual.names)):
        exp = EXPECTED_ACTIVITY_SCHEMA.get(col)
        act = actual.field(col).type if col in actual.names else None
        rows.append({
            "column": col,
            "expected": str(exp) if exp is not None else None,
            "actual": str(act) if act is not None else None,
            "ok": exp is not None and act is not None and exp.equals(act),
        })
    return pd.DataFrame(rows)


def read_activity() -> pd.DataFrame:
    """Read ``agent_activity.parquet`` keeping nullable integers nullable.

    A plain ``to_pandas()`` turns an int64 column with nulls into float64, which
    would hide that e.g. ``sequence_number`` is missing. Mapping int64 to pandas'
    ``Int64`` keeps both the integer type and the nulls.
    """
    table = pq.read_table(path_of("agent_activity"))
    df = table.to_pandas(types_mapper={pa.int64(): pd.Int64Dtype()}.get)
    return _with_provenance(df, FILES["agent_activity"][0], range(1, len(df) + 1))


# --------------------------------------------------------------------------- JSON

def _record_start_lines(path: Path, key: str, expected: int) -> list[int]:
    """Physical line numbers of each record in a pretty-printed JSON array.

    Each record carries ``key`` exactly once, so the n-th line containing ``"key":``
    is where the n-th record starts. If the count disagrees (e.g. a minified file)
    the caller falls back to the 1-based array index.
    """
    needle = f'"{key}":'
    lines = [i for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1) if needle in line]
    return lines if len(lines) == expected else []


def read_sandbox_policies_raw() -> list[dict]:
    return json.loads(path_of("sandbox_policies").read_text(encoding="utf-8"))


def read_sandbox_policies() -> pd.DataFrame:
    """Flatten the nested policy array to one row per (profile, version).

    ``json_normalize`` turns ``network.egress_allowlist`` etc. into columns. Lists
    (allowed tools, allowlists, ports, scopes) stay as Python lists. ``has_network_block``
    records whether the ``network`` key exists at all: per the spec a policy version
    with no network block allows **no** egress, which is not the same as an empty
    allowlist, so flattening must not lose that distinction.
    """
    raw = read_sandbox_policies_raw()
    df = pd.json_normalize(raw, sep="_")
    df["has_network_block"] = ["network" in r and r["network"] is not None for r in raw]
    lines = _record_start_lines(path_of("sandbox_policies"), "sandbox_profile_id", len(raw)) or range(1, len(raw) + 1)
    return _with_provenance(df, FILES["sandbox_policies"][0], lines)


@dataclass
class SiemExport:
    envelope: dict
    alerts: pd.DataFrame


def read_siem_alerts() -> SiemExport:
    """Unwrap the export envelope and flatten each alert's nested objects."""
    doc = json.loads(path_of("siem_alerts").read_text(encoding="utf-8"))
    envelope = {k: v for k, v in doc.items() if k != "alerts"}
    alerts = pd.json_normalize(doc["alerts"], sep="_")
    lines = _record_start_lines(path_of("siem_alerts"), "alert_id", len(alerts)) or range(1, len(alerts) + 1)
    return SiemExport(envelope, _with_provenance(alerts, FILES["siem_alerts"][0], lines))


# ------------------------------------------------------------------------- Syslog

@dataclass
class AuditLog:
    parsed: pd.DataFrame
    unparsed: pd.DataFrame


def read_sandbox_audit() -> AuditLog:
    """Regex-parse the syslog file line by line.

    Values are kept as raw strings, including the ``-`` null marker and the naive,
    year-less ``log_time``; normalising them is the cleaning stage's job. Lines that
    fail the anchored regex (or are not valid UTF-8) are returned separately with
    their raw text so they can become ``UNPARSEABLE_RECORD`` rejects.
    """
    with open(path_of("sandbox_audit"), "rb") as fh:
        return parse_audit_lines(fh)


def parse_audit_lines(lines, file_name: str = FILES["sandbox_audit"][0]) -> AuditLog:
    """Parse an iterable of raw syslog lines (bytes); line numbers start at 1."""
    parsed, unparsed = [], []
    for line_no, raw in enumerate(lines, 1):
        try:
            text = raw.decode("utf-8").rstrip("\n")
        except UnicodeDecodeError:
            unparsed.append({"source_line": line_no, "raw_line": raw.decode("utf-8", "replace").rstrip("\n"),
                             "problem": "not_utf8"})
            continue
        m = AUDIT_LINE_RE.match(text)
        if m:
            parsed.append({"source_line": line_no, **m.groupdict(), "raw_line": text})
        else:
            unparsed.append({"source_line": line_no, "raw_line": text, "problem": "regex_no_match"})
    parsed_df = pd.DataFrame(parsed, columns=["source_line", *AUDIT_LINE_RE.groupindex, "raw_line"])
    unparsed_df = pd.DataFrame(unparsed, columns=["source_line", "raw_line", "problem"])
    parsed_df.insert(0, "source_file", file_name)
    unparsed_df.insert(0, "source_file", file_name)
    return AuditLog(parsed_df, unparsed_df)


# ---------------------------------------------------------------------- Inventory

def file_inventory() -> pd.DataFrame:
    """Byte-level facts about every file: size, lines, encoding, line endings."""
    rows = []
    for name, (file_name, fmt) in FILES.items():
        data = path_of(name).read_bytes()
        row = {"file": file_name, "format": fmt,
               "used_in": "M2" if name in ("stream_events", "incident_labels") else "M1",
               "bytes": len(data)}
        if fmt == "parquet":
            # binary: line/encoding facts are meaningless, report the footer instead
            meta = pq.ParquetFile(path_of(name)).metadata
            row.update({"rows_in_footer": meta.num_rows, "row_groups": meta.num_row_groups,
                        "compression": meta.row_group(0).column(0).compression})
        else:
            try:
                data.decode("utf-8")
                utf8 = True
            except UnicodeDecodeError:
                utf8 = False
            row.update({"lines": data.count(b"\n") + (0 if data.endswith(b"\n") or not data else 1),
                        "crlf_lines": data.count(b"\r\n"), "utf8": utf8,
                        "ends_with_newline": data.endswith(b"\n")})
        rows.append(row)
    return pd.DataFrame(rows)
