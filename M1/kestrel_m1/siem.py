"""Stage 2 for siem_alerts.json: flatten, validate, convert detected_at to UTC.

Each source_system writes its own local offset (corp-siem +02:00, egress-proxy +03:00,
cloud-ids -05:00). Profiling section 10 showed the offsets are honest: after
conversion, alerts trail their flows by 0-45 s for every system. So the stated offset
is applied as written, and kept in ``utc_offset`` for provenance.

``severity`` is the targeted system's opinion and is carried, never re-scored.
"""

from __future__ import annotations

import json

import pandas as pd

from . import readers
from .rejects import StageResult, build_rejects, check_rejects, first_reason, join_details, summarise
from .timeutil import ISO_RX, parse_iso_utc

SOURCE_FORMAT = "nested_json"

REQUIRED = ["alert_id", "source_system", "severity", "detected_at", "source_ip", "destination_host",
            "destination_port", "technique_id", "rule", "confidence"]
SEVERITIES = {"low", "medium", "high", "critical"}
IPV4_RX = r"(?:\d{1,3}\.){3}\d{1,3}"
OFFSET_ISO_RX = ISO_RX.replace("(?:Z|", "(?:")                 # an explicit +hh:mm offset is required here

ACCEPTED_COLUMNS = [
    "source_file", "source_line", "alert_id", "source_system", "severity", "detected_at_utc", "detected_at_raw",
    "utc_offset", "source_ip", "destination_host", "destination_port", "technique_id", "technique_name", "rule",
    "confidence", "quality_flags",
]


def _raw_payloads() -> list[str]:
    """Each alert exactly as exported (nested), one JSON string per alert, in file order."""
    doc = json.loads(readers.path_of("siem_alerts").read_text(encoding="utf-8"))
    return [json.dumps(a, ensure_ascii=False) for a in doc["alerts"]]


def judge(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    checks = {}
    checks["DUPLICATE_EVENT_ID"] = (df.alert_id.notna() & df.alert_id.duplicated(keep="first"),
                                    "alert_id repeats an earlier alert")
    text = df.detected_at.astype("string")
    bad_ts = df.detected_at.notna() & (df.detected_at_utc.isna() | ~text.str.fullmatch(OFFSET_ISO_RX).fillna(False))
    checks["MALFORMED_TIMESTAMP"] = (bad_ts, "detected_at='" + text + "' (needs an explicit offset)")
    failed, detail = join_details({c: df[c].isna() for c in REQUIRED}, idx)
    checks["MISSING_REQUIRED_FIELD"] = (failed, "null: " + detail)
    port = pd.to_numeric(df.destination_port, errors="coerce")
    conf = pd.to_numeric(df.confidence, errors="coerce")
    failed, detail = join_details({"destination_port": (port < 1) | (port > 65535),
                                   "confidence": (conf < 0) | (conf > 1)}, idx)
    checks["INVALID_NUMERIC_RANGE"] = (failed, "out of range: " + detail)
    schema = {"severity": df.severity.notna() & ~df.severity.isin(SEVERITIES),
              "alert_id": df.alert_id.notna() & ~df.alert_id.astype("string").str.fullmatch(r"al_\w+").fillna(False)}
    for c in ["source_ip", "destination_host"]:
        schema[c] = df[c].notna() & ~df[c].astype("string").str.fullmatch(IPV4_RX).fillna(False)
    failed, detail = join_details(schema, idx)
    checks["SCHEMA_VIOLATION"] = (failed, "value outside contract: " + detail)
    return first_reason(checks, idx)


def clean_siem(export: readers.SiemExport | None = None) -> StageResult:
    """Validate alerts; rejects carry the alert's original nested JSON when read from the file."""
    from_file = export is None
    export = readers.read_siem_alerts() if from_file else export
    raw = export.alerts.copy()
    df = raw.copy()
    df["detected_at_utc"] = parse_iso_utc(df.detected_at)
    verdict = judge(df)
    if from_file:
        raw["raw_payload"] = _raw_payloads()
    rejects = build_rejects(raw, verdict, SOURCE_FORMAT, "alert_id", payload_col="raw_payload" if from_file else None)

    a = df[verdict.reason.isna()].copy()
    a["detected_at_raw"] = a.detected_at
    a["utc_offset"] = a.detected_at.astype("string").str.extract(r"([+-]\d{2}:\d{2}|Z)$", expand=False)
    a["destination_port"] = a.destination_port.astype("int64")
    a["confidence"] = a.confidence.astype("float64")
    a["quality_flags"] = ""
    a = a[ACCEPTED_COLUMNS].sort_values(["detected_at_utc", "alert_id"]).reset_index(drop=True)

    check_rejects(rejects)
    return StageResult(a, rejects, summarise(len(raw), len(a), rejects))
