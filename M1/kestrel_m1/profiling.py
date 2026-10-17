"""Reusable profiling helpers used by notebooks/01_profiling.ipynb.

They only *describe* data; nothing here repairs or drops anything.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

PROVENANCE = ("source_file", "source_line")


def profile_frame(df: pd.DataFrame, top: int = 3) -> pd.DataFrame:
    """One row per column: dtype, nulls, empty strings, cardinality, top values."""
    rows = []
    for col in df.columns:
        if col in PROVENANCE:
            continue
        s = df[col]
        # only object columns can hold lists (flattened JSON); make them hashable for counting
        hashable = s.map(lambda v: tuple(v) if isinstance(v, list) else v) if s.dtype == object else s
        non_null = hashable.dropna()
        is_text = pd.api.types.is_string_dtype(s) or s.dtype == object
        vc = non_null.value_counts()
        rows.append({
            "column": col,
            "dtype": str(s.dtype),
            "nulls": int(s.isna().sum()),
            "null_rate": round(float(s.isna().mean()), 4),
            "empty_str": int((s == "").sum()) if is_text else 0,
            "distinct": int(non_null.nunique()),
            "top_values": ", ".join(f"{k} ({v})" for k, v in vc.head(top).items()),
        })
    return pd.DataFrame(rows)


def value_shapes(s: pd.Series, top: int = 10) -> pd.Series:
    """Collapse each value to its *shape*: digits to 9, letters to a/A.

    ``2026-03-14T09:22:01.412Z`` and ``1772435438897`` look very different as
    shapes, so a column mixing formats shows several shapes at once.
    """
    shaped = (s.dropna().astype(str)
              .str.replace(r"\d", "9", regex=True)
              .str.replace(r"[a-z]", "a", regex=True)
              .str.replace(r"[A-Z]", "A", regex=True))
    return shaped.value_counts().head(top)


def enum_violations(s: pd.Series, allowed) -> pd.Series:
    """Counts of non-null values that are outside the documented enum."""
    s = s.dropna()
    return s[~s.isin(set(allowed))].value_counts()


def fk_orphans(child: pd.Series, parent: pd.Series) -> pd.Series:
    """Counts of non-null child keys that have no parent row."""
    c = child.dropna()
    return c[~c.isin(set(parent.dropna()))].value_counts()


def numeric_ranges(df: pd.DataFrame, cols) -> pd.DataFrame:
    """min / max / negatives / zeros per numeric column."""
    rows = []
    for col in cols:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        rows.append({"column": col, "count": len(s), "min": s.min(), "p50": s.median(),
                     "p99": s.quantile(0.99), "max": s.max(),
                     "negative": int((s < 0).sum()), "zero": int((s == 0).sum())})
    return pd.DataFrame(rows)


def interval_overlaps(df: pd.DataFrame, key: str, start: str, end: str) -> pd.DataFrame:
    """Pairs of rows with the same ``key`` whose [start, end) windows overlap.

    A null ``end`` means "still valid" (open-ended).
    """
    out = []
    far = pd.Timestamp.max.tz_localize("UTC")
    for k, g in df.groupby(key):
        g = g.sort_values(start)
        recs = g.to_dict("records")
        for i, a in enumerate(recs):
            for b in recs[i + 1:]:
                a_end = a[end] if pd.notna(a[end]) else far
                b_end = b[end] if pd.notna(b[end]) else far
                if a[start] < b_end and b[start] < a_end:
                    out.append({key: k, "a": a, "b": b,
                                "overlap_from": max(a[start], b[start]),
                                "overlap_to": min(a_end, b_end)})
    return pd.DataFrame(out)


@dataclass
class FindingsLog:
    """Collects data-quality findings while profiling, before anything is fixed."""

    rows: list = field(default_factory=list)

    def add(self, fid: str, source: str, problem: str, detected_by: str, count, planned_decision: str):
        self.rows.append({"id": fid, "source": source, "problem": problem, "detected_by": detected_by,
                          "count": count, "planned_decision": planned_decision})

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)
