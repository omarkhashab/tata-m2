"""The closed rejection enum and the rejects table shared by every source.

Dataset Description section 4: every refused record gets **exactly one** reason from
this closed list; when several apply, the first one in this order wins. The same
order is reused by the M2 Kafka consumer, so it lives here and nowhere else.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

REASONS = [
    "DUPLICATE_EVENT_ID",
    "UNKNOWN_AGENT_ID",
    "UNKNOWN_TENANT_ID",
    "MALFORMED_TIMESTAMP",
    "ORPHAN_EVENT",
    "INVALID_EVENT_TYPE",
    "MISSING_REQUIRED_FIELD",
    "INVALID_NUMERIC_RANGE",
    "LATE_BEYOND_WATERMARK",
    "UNPARSEABLE_RECORD",
    "SCHEMA_VIOLATION",
]

REJECT_COLUMNS = ["source_file", "source_format", "source_line", "record_id", "reason", "reason_detail",
                  "raw_payload"]


def first_reason(checks: dict[str, tuple[pd.Series, pd.Series | str]], index) -> pd.DataFrame:
    """Pick, per row, the first failing check in enum order.

    ``checks`` maps a reason to ``(failed_mask, detail)`` where detail is a per-row
    string Series or one string for all rows. Reasons that are not in the enum raise,
    so a typo can never invent a new reason. Returns columns ``reason`` (<NA> when the
    row passes every check) and ``reason_detail``.
    """
    unknown = set(checks) - set(REASONS)
    if unknown:
        raise ValueError(f"not in the closed rejection enum: {sorted(unknown)}")
    reason = pd.Series(pd.NA, index=index, dtype="string")
    detail = pd.Series(pd.NA, index=index, dtype="string")
    for r in REASONS:                                   # enum order = priority order
        if r not in checks:
            continue
        failed, why = checks[r]
        take = pd.Series(failed, index=index).fillna(False).astype(bool) & reason.isna()
        if not take.any():
            continue
        reason[take] = r
        detail[take] = why[take] if isinstance(why, pd.Series) else why
    return pd.DataFrame({"reason": reason, "reason_detail": detail})


def payload_json(raw: pd.DataFrame) -> pd.Series:
    """Each raw record as one JSON object, exactly as read (nulls stay null).

    Provenance columns are left out: they are columns of the rejects table itself.
    """
    body = raw.drop(columns=[c for c in ("source_file", "source_line") if c in raw.columns])
    if body.empty:
        return pd.Series([], index=raw.index, dtype="string")
    lines = body.to_json(orient="records", lines=True, date_format="iso", force_ascii=False).splitlines()
    return pd.Series(lines, index=raw.index, dtype="string")


def build_rejects(raw: pd.DataFrame, verdict: pd.DataFrame, source_format: str, id_col: str | None,
                  payload_col: str | None = None) -> pd.DataFrame:
    """Rows of ``raw`` whose ``verdict.reason`` is set, in the rejects-table shape.

    The payload is the record as JSON, or, for line-oriented text sources, the
    original line itself (``payload_col``), which is the most faithful raw form.
    """
    rej = verdict.reason.notna()
    r = raw[rej]
    out = pd.DataFrame({
        "source_file": r.source_file,
        "source_format": source_format,
        "source_line": r.source_line.astype("int64"),
        "record_id": r[id_col].astype("string") if id_col else pd.Series(pd.NA, index=r.index, dtype="string"),
        "reason": verdict.reason[rej],
        "reason_detail": verdict.reason_detail[rej],
        "raw_payload": r[payload_col].astype("string") if payload_col else payload_json(r),
    })
    return out.reset_index(drop=True)[REJECT_COLUMNS]


@dataclass
class StageResult:
    accepted: pd.DataFrame
    rejects: pd.DataFrame
    summary: pd.DataFrame          # one row per outcome: ACCEPTED + each reason, with counts


def summarise(received: int, accepted: int, rejects: pd.DataFrame) -> pd.DataFrame:
    """Outcome counts in enum order; asserts that nothing was dropped silently."""
    assert accepted + len(rejects) == received, f"{received} received != {accepted} accepted + {len(rejects)} rejected"
    counts = rejects.reason.value_counts()
    order = [r for r in REASONS if r in counts.index]
    out = pd.DataFrame({"outcome": ["ACCEPTED"] + order,
                        "rows": [accepted] + [int(counts[r]) for r in order]})
    out["share_pct"] = (100 * out.rows / max(received, 1)).round(3)
    return out


def join_details(masks: dict[str, pd.Series], index) -> tuple[pd.Series, pd.Series]:
    """Combine per-field failure masks into (any failed, ``"field1,field2"`` detail)."""
    detail = pd.Series("", index=index, dtype="string")
    for col, m in masks.items():
        m = pd.Series(m, index=index).fillna(False).astype(bool)
        detail = detail.where(~m, detail.where(detail == "", detail + ",") + col)
    return detail != "", detail


def check_rejects(rejects: pd.DataFrame) -> None:
    """Invariants of fact_rejected_record: one enum reason and full provenance per row."""
    assert list(rejects.columns) == REJECT_COLUMNS, rejects.columns
    assert rejects.reason.isin(REASONS).all(), "reason outside the closed enum"
    for c in ("source_file", "source_line", "raw_payload"):
        assert rejects[c].notna().all(), f"reject without {c}"
    assert not rejects.duplicated(["source_file", "source_line"]).any(), "a source record was rejected twice"
