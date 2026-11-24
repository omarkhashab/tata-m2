"""Lab 6: a catalogue of every source file the warehouse is built from.

One row per file *content*: path, format, size, SHA-256 content hash, record count and
ingestion time. The key ``source_file_id = <file name>@<first 12 hex of sha256>`` names
the exact bytes a row came from, so every warehouse row that carries
``(source_file_id, source_line)`` points back at one line of one specific file. If a
file changes, it gets a new id and a new catalogue row; the old row stays.

``ingested_at`` is the first time this content was ingested: the load writes the row
with ``ON CONFLICT DO NOTHING`` so a re-run keeps the original time (and the load stays
idempotent).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from kestrel_m1 import readers

from . import config

FORMAT = {"csv": "csv", "parquet": "parquet", "nested_json": "json", "json_array": "json", "syslog": "syslog"}
GRAIN = {
    "tenants.csv": "one tenant", "agents.csv": "one agent version", "tools.csv": "one tool version",
    "model_pricing.csv": "one model price point", "sandbox_policies.json": "one policy version",
    "agent_activity.parquet": "one activity event", "network_flows.csv": "one egress attempt",
    "sandbox_audit.log": "one runtime decision (line)", "siem_alerts.json": "one alert",
    "incident_labels.csv": "one labelled session", "stream_events.json": "one live activity event (array element)",
}


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def record_count(path: Path, fmt: str) -> int:
    """Records as the reader sees them (not bytes or physical lines)."""
    if fmt == "parquet":
        return pq.ParquetFile(path).metadata.num_rows
    data = path.read_bytes()
    if fmt == "csv":
        return data.count(b"\n") - 1                  # header row; no embedded newlines (M1 inventory)
    if fmt == "syslog":
        return data.count(b"\n") + (0 if data.endswith(b"\n") else 1)
    doc = json.loads(data)
    if isinstance(doc, dict) and "alerts" in doc:
        return len(doc["alerts"])
    return len(doc)


def build_catalog(raw_dir: Path | None = None, now: datetime | None = None) -> pd.DataFrame:
    raw_dir = Path(raw_dir or config.RAW_DIR)
    now = now or datetime.now(timezone.utc)
    rows = []
    for name, (file_name, kind) in readers.FILES.items():
        path = raw_dir / file_name
        digest = sha256_of(path)
        try:
            rel = path.resolve().relative_to(config.REPO_DIR.resolve()).as_posix()
        except ValueError:
            rel = path.as_posix()
        rows.append({
            "source_file_id": f"{file_name}@{digest[:12]}",
            "file_name": file_name,
            "path": rel,
            "format": FORMAT[kind],
            "format_detail": kind,
            "grain": GRAIN[file_name],
            "size_bytes": path.stat().st_size,
            "content_sha256": digest,
            "row_count": record_count(path, kind),
            "used_in": "M2" if name in ("stream_events", "incident_labels") else "M1+M2",
            "ingested_at": pd.Timestamp(now).tz_convert("UTC"),
        })
    df = pd.DataFrame(rows)
    df["ingested_at"] = df.ingested_at.astype("datetime64[us, UTC]")
    return df


def id_by_file(catalog: pd.DataFrame) -> dict[str, str]:
    return dict(zip(catalog.file_name, catalog.source_file_id))
