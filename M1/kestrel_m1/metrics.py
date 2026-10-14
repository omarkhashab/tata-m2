"""Milestone 1 metrics 1, 2, 8 and 9 over the curated output (Dataset Description, Appendix A).

    python -m kestrel_m1.metrics       # reads output/curated, writes output/metrics/

Every number is computed from the curated tables the pipeline writes (accepted rows
only, after dedup and rejects). ``POLICIES`` states, per metric, the denominator, the
population filter, the late-event policy and the null policy, as Appendix A requires.

Shared building block: ``session_outcomes`` gives one row per *terminal* session (an
accepted ``session.end`` or ``session.error``) with its outcome, derived duration,
``latency_ms`` cross-check and trace-completeness inputs. Metrics 1, 2 and 8 all
read from it, so they share one population.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .activity import START, TERMINAL
from .timeutil import LATE_THRESHOLD_S

ROOT = Path(__file__).resolve().parents[1]
CURATED = ROOT / "output" / "curated"
METRICS_OUT = ROOT / "output" / "metrics"

LATENCY_TOLERANCE_S = 1.0       # terminal latency_ms vs derived duration: "disagree" beyond this
QUANTILES = {"p50": 0.50, "p95": 0.95}

ACTIVITY_COLS = ["event_id", "event_type", "sequence_number", "session_id", "event_time_utc", "lateness_s",
                 "is_late", "latency_ms", "tokens_in", "tokens_out", "input_price_per_1k", "output_price_per_1k",
                 "cost_usd"]
TABLES = {
    "stg_agent_activity": ACTIVITY_COLS,
    "stg_network_flows": ["flow_id", "src_ip"],
    "stg_siem_alerts": ["alert_id", "source_system", "source_ip"],
    "siem_correlation": None,
    "correlation_sensitivity": None,
    "stage_summary": None,
    "fact_rejected_record": ["source_file", "source_line", "reason", "raw_payload"],
}

POLICIES = pd.DataFrame([
    {"metric": "1 session volume / success / error rate",
     "denominator": "distinct session_id with an accepted session.end or session.error row",
     "population": "accepted activity rows (after dedup and rejects). Every terminal session has an accepted "
                   "session.start because orphans are rejected. Sessions with no terminal row are excluded.",
     "late_policy": "late rows are accepted and only flagged (the batch has no watermark), so a late terminal "
                    "row still makes its session terminal",
     "null_policy": "none possible: event_type, session_id and event_time are required fields, and rows "
                    "missing them are rejected"},
    {"metric": "2 p50 / p95 session duration",
     "denominator": "terminal sessions (same population as metric 1)",
     "population": "duration = terminal event_time_utc - session.start event_time_utc; pandas "
                   "Series.quantile with linear interpolation between closest ranks",
     "late_policy": "included. Lateness concerns ingest_time, and the duration uses event_time only",
     "null_policy": "a session without an accepted start would have no duration and be excluded; the curated data "
                    "has none. latency_ms is never used as the duration, only as a cross-check (disagreement "
                    "> 1 s reported)"},
    {"metric": "8a trace completeness",
     "denominator": "terminal sessions (same population as metric 1)",
     "population": "complete = at least one accepted llm.turn, and the accepted sequence_numbers are exactly "
                   "1..max (no gap, no repeat). A row rejected for any reason except DUPLICATE_EVENT_ID (which "
                   "leaves one accepted copy) leaves a gap, so the session counts as incomplete.",
     "late_policy": "late rows are accepted, so they fill their sequence slot",
     "null_policy": "sequence_number is required; rows without it are rejected, which leaves a gap"},
    {"metric": "8b correlation coverage",
     "denominator": "all accepted SIEM alerts",
     "population": "covered = the alert is linked to exactly one session at N = 60 s: matched (one candidate "
                   "flow) plus ambiguous alerts whose candidate flows all belong to one session. Ambiguous "
                   "alerts spanning several sessions, and unmatched alerts, are not covered. Strict "
                   "(matched only) and on-platform variants are reported next to it.",
     "late_policy": "not applicable: alerts and flows carry no ingest_time; |delta t| <= N is the only time rule",
     "null_policy": "an alert without a session is not covered; it is never force-matched"},
    {"metric": "9a rejected-record rate",
     "denominator": "received records per source = accepted + rejected (every received record lands in "
                    "exactly one of the two; the pipeline asserts this)",
     "population": "the four event sources. Headline = agent_activity.parquet; the overall rate is pooled "
                   "over all four. Reference files are cleaned by dedup and repair, not rejected, so they are "
                   "not counted.",
     "late_policy": "late rows are not rejected in batch (no LATE_BEYOND_WATERMARK), so lateness does not "
                    "raise this rate",
     "null_policy": "a null in a required field is a reject (MISSING_REQUIRED_FIELD) and is counted"},
    {"metric": "9b late-event rate",
     "denominator": "accepted agent_activity rows",
     "population": "late = ingest_time_utc - event_time_utc > 10 s. Only agent_activity has an ingest_time, so "
                   "the other sources have no lateness. Late duplicate copies were already removed by dedup "
                   "(the earliest ingest is kept) and are not counted.",
     "late_policy": "this metric is the late policy's measurement: late rows are kept and flagged LATE",
     "null_policy": "none possible: both timestamps are required and a malformed one is a reject"},
    {"metric": "missing token counts",
     "denominator": "llm.turn rows",
     "population": "turns whose output_tokens is null",
     "late_policy": "-",
     "null_policy": "null-excluded: the count stays null, cost_usd is null and both are left out of sums and "
                    "averages, never imputed as 0 (effect shown by missing_token_effect)"},
])


def load_curated(curated: Path = CURATED) -> dict[str, pd.DataFrame]:
    """The curated tables the metrics read (only the columns they need)."""
    curated = Path(curated)
    missing = [t for t in TABLES if not (curated / f"{t}.parquet").exists()]
    if missing:
        raise FileNotFoundError(f"run `python -m kestrel_m1.pipeline --no-db` first; missing {missing} in {curated}")
    return {t: pd.read_parquet(curated / f"{t}.parquet", columns=cols) for t, cols in TABLES.items()}


# ------------------------------------------------------------- terminal sessions

def session_outcomes(activity: pd.DataFrame, include_late: bool = True) -> pd.DataFrame:
    """One row per terminal session: outcome, duration, latency cross-check, trace inputs.

    ``include_late=False`` drops LATE rows first. That is not the declared policy, but
    a sensitivity check showing what the late policy changes.
    """
    a = activity if include_late else activity[~activity.is_late.astype(bool)]
    term = a[a.event_type.isin(TERMINAL)].set_index("session_id")
    assert term.index.is_unique, "a session with more than one terminal row"
    start = a[a.event_type == START].set_index("session_id").event_time_utc

    s = pd.DataFrame({"outcome": term.event_type.map({"session.end": "success", "session.error": "error"}),
                      "start_time_utc": start.reindex(term.index), "end_time_utc": term.event_time_utc,
                      "terminal_latency_s": term.latency_ms.astype("Float64") / 1000})
    s["duration_s"] = (s.end_time_utc - s.start_time_utc).dt.total_seconds()
    s["latency_disagrees"] = (s.terminal_latency_s - s.duration_s).abs() > LATENCY_TOLERANCE_S

    seq = a.groupby("session_id").sequence_number.agg(n_events="size", n_seq="nunique", min_seq="min",
                                                      max_seq="max")
    s = s.join(seq)
    s["n_turns"] = a[a.event_type == "llm.turn"].groupby("session_id").size().reindex(s.index, fill_value=0)
    s["has_gap"] = (s.min_seq != 1) | (s.max_seq != s.n_seq) | (s.n_seq != s.n_events)
    s["trace_complete"] = s.start_time_utc.notna() & (s.n_turns >= 1) & ~s.has_gap
    return s.rename_axis("session_id").reset_index()


def _row(metric: str, value, numerator=None, denominator=None, unit: str = "") -> dict:
    return {"metric": metric, "value": value, "numerator": numerator, "denominator": denominator, "unit": unit}


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).astype({"numerator": "Int64", "denominator": "Int64"})


def _rate(metric: str, numerator: int, denominator: int) -> dict:
    return _row(metric, round(100 * numerator / denominator, 3) if denominator else None,
                int(numerator), int(denominator), "%")


# ----------------------------------------------------------------------- metrics

def metric_1(outcomes: pd.DataFrame) -> pd.DataFrame:
    n = len(outcomes)
    return _frame([
        _row("1 terminal sessions (volume)", n, unit="sessions"),
        _rate("1 success rate", (outcomes.outcome == "success").sum(), n),
        _rate("1 error rate", (outcomes.outcome == "error").sum(), n),
    ])


def metric_2(outcomes: pd.DataFrame) -> pd.DataFrame:
    d = outcomes.duration_s.dropna()
    rows = [_row(f"2 {name} session duration", round(float(d.quantile(q, interpolation="linear")), 3),
                 denominator=len(d), unit="s") for name, q in QUANTILES.items()]
    rows.append(_rate(f"2 cross-check: terminal latency_ms disagrees > {LATENCY_TOLERANCE_S:g} s",
                      outcomes.latency_disagrees.sum(), outcomes.latency_disagrees.notna().sum()))
    return _frame(rows)


def metric_8_trace(outcomes: pd.DataFrame) -> pd.DataFrame:
    return _frame([_rate("8 trace completeness", outcomes.trace_complete.sum(), len(outcomes))])


def metric_8_coverage(correlation: pd.DataFrame, alerts: pd.DataFrame, flows: pd.DataFrame) -> pd.DataFrame:
    """Primary = alerts with a session; strict = single-flow matches; on-platform = only
    alerts whose source IP belongs to some Kestrel sandbox (appears as a flow src_ip)."""
    c = correlation.merge(alerts[["alert_id", "source_ip"]], on="alert_id", how="left", validate="one_to_one")
    covered = c.session_id.notna()
    on_platform = c.source_ip.isin(set(flows.src_ip))
    return _frame([
        _rate("8 correlation coverage (alert -> session)", covered.sum(), len(c)),
        _rate("8 correlation coverage, strict (single-flow match only)", (c.match_status == "matched").sum(), len(c)),
        _rate("8 correlation coverage, on-platform alerts only", (covered & on_platform).sum(), on_platform.sum()),
    ])


def metric_9_rejected(stage_summary: pd.DataFrame) -> pd.DataFrame:
    s = stage_summary.assign(rejected=stage_summary.rows.where(stage_summary.outcome != "ACCEPTED", 0))
    per = s.groupby("source_file", sort=False).agg(received=("rows", "sum"), rejected=("rejected", "sum"))
    rows = [_rate(f"9 rejected rate: {f}", r.rejected, r.received) for f, r in per.iterrows()]
    rows.append(_rate("9 rejected rate: all four sources", per.rejected.sum(), per.received.sum()))
    return _frame(rows)


def metric_9_late(activity: pd.DataFrame) -> pd.DataFrame:
    return _frame([_rate(f"9 late-event rate (lateness > {LATE_THRESHOLD_S} s)",
                               activity.is_late.sum(), len(activity))])


def compute_all(t: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Every M1 metric as one tidy table (metric, value, numerator, denominator, unit)."""
    o = session_outcomes(t["stg_agent_activity"])
    return pd.concat([
        metric_1(o), metric_2(o), metric_8_trace(o),
        metric_8_coverage(t["siem_correlation"], t["stg_siem_alerts"], t["stg_network_flows"]),
        metric_9_rejected(t["stage_summary"]), metric_9_late(t["stg_agent_activity"]),
    ], ignore_index=True)


# ------------------------------------------------------------ breakdowns / policy effects

def rejected_slots(rejects: pd.DataFrame, source_file: str = "agent_activity.parquet",
                   exclude=("DUPLICATE_EVENT_ID",)) -> set[tuple[str, int]]:
    """(session_id, sequence_number) of every rejected activity row that left its slot
    empty. A duplicate leaves its kept copy, so it is excluded."""
    r = rejects[(rejects.source_file == source_file) & ~rejects.reason.isin(exclude)]
    slots = set()
    for p in r.raw_payload:
        row = json.loads(p)
        if row.get("session_id") is not None and row.get("sequence_number") is not None:
            slots.add((row["session_id"], int(row["sequence_number"])))
    return slots


def trace_breakdown(activity: pd.DataFrame, outcomes: pd.DataFrame, rejects: pd.DataFrame) -> pd.DataFrame:
    """Why terminal sessions are incomplete, one cause per session.

    For each gap, the missing sequence numbers are compared with the rejected rows: the
    gap was *left by a reject* when every missing slot was rejected, *never sent* when
    none was, and *both* otherwise. A session with no llm.turn is reported as such.
    """
    slots = rejected_slots(rejects)
    gapped = outcomes.loc[outcomes.has_gap, "session_id"]
    seqs = activity[activity.session_id.isin(gapped)].groupby("session_id").sequence_number.agg(set)
    cause = {}
    for sid, have in seqs.items():
        missing = set(range(1, max(have) + 1)) - have
        n_rejected = sum((sid, n) in slots for n in missing)
        cause[sid] = ("gap left by rejected rows" if n_rejected == len(missing) else
                      "gap in source (never sent)" if n_rejected == 0 else "gap: both causes")
    o = outcomes
    status = o.session_id.map(cause).fillna("complete")
    status[o.n_turns == 0] = "no llm.turn"
    assert ((status == "complete") == o.trace_complete).all()
    out = status.value_counts().rename_axis("trace_status").reset_index(name="sessions")
    out["share_pct"] = (100 * out.sessions / len(o)).round(3)
    return out


def rejects_by_reason(rejects: pd.DataFrame) -> pd.DataFrame:
    """Reject counts per source and reason, in the enum's priority order."""
    from .rejects import REASONS
    r = rejects.groupby(["source_file", "reason"]).size().rename("rejected").reset_index()
    r["reason"] = pd.Categorical(r.reason, categories=REASONS, ordered=True)
    return r.sort_values(["source_file", "reason"], ignore_index=True)


def late_by_event_type(activity: pd.DataFrame) -> pd.DataFrame:
    g = activity.groupby("event_type").agg(accepted=("is_late", "size"), late=("is_late", "sum"),
                                           p99_lateness_s=("lateness_s", lambda x: x.quantile(0.99)),
                                           max_lateness_s=("lateness_s", "max"))
    g["late_pct"] = (100 * g.late / g.accepted).round(3)
    return g.reset_index()


def late_policy_effect(activity: pd.DataFrame) -> pd.DataFrame:
    """Metrics 1, 2, 8a with late rows kept (declared policy) vs dropped (sensitivity)."""
    cols = {}
    for label, keep in (("late kept (declared)", True), ("late dropped", False)):
        o = session_outcomes(activity, include_late=keep)
        m = pd.concat([metric_1(o), metric_2(o).iloc[:2], metric_8_trace(o)], ignore_index=True)
        cols[label] = m.set_index("metric").value
    out = pd.DataFrame(cols)
    out["difference"] = out.iloc[:, 1] - out.iloc[:, 0]
    return out.reset_index()


def missing_token_effect(activity: pd.DataFrame) -> pd.DataFrame:
    """Null-excluded (declared) vs zero-imputed output_tokens, on two token metrics.

    Zero-imputation is applied the way a naive pipeline would: missing output_tokens
    become 0 and the turn's cost is then its input-token cost alone.
    """
    t = activity[activity.event_type == "llm.turn"]
    out_zero = t.tokens_out.fillna(0)
    cost_zero = (t.tokens_in.astype("Float64") / 1000 * t.input_price_per_1k
                 + out_zero.astype("Float64") / 1000 * t.output_price_per_1k).round(6)
    rows = [
        ("llm.turn rows", len(t), len(t)),
        ("turns with output_tokens", int(t.tokens_out.notna().sum()), len(t)),
        ("mean output_tokens per turn", round(float(t.tokens_out.mean()), 3), round(float(out_zero.mean()), 3)),
        ("total output_tokens", int(t.tokens_out.sum()), int(out_zero.sum())),
        ("turns with a cost", int(t.cost_usd.notna().sum()), int(cost_zero.notna().sum())),
        ("estimated LLM cost, USD", round(float(t.cost_usd.sum()), 2), round(float(cost_zero.sum()), 2)),
        ("mean cost per 1,000 costed turns, USD", round(1000 * float(t.cost_usd.mean()), 3),
         round(1000 * float(cost_zero.mean()), 3)),
    ]
    out = pd.DataFrame(rows, columns=["quantity", "null_excluded (declared)", "zero_imputed"])
    out["difference_pct"] = (100 * (out.zero_imputed - out.iloc[:, 1]) / out.iloc[:, 1]).round(3)
    return out


# --------------------------------------------------------------------------- CLI

def write_outputs(t: dict[str, pd.DataFrame], out: Path = METRICS_OUT) -> pd.DataFrame:
    """Write the metric values, their policies and the missing-token effect as CSV."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    values = compute_all(t)
    values.to_csv(out / "m1_metrics.csv", index=False)
    POLICIES.to_csv(out / "m1_metric_policies.csv", index=False)
    missing_token_effect(t["stg_agent_activity"]).to_csv(out / "missing_token_effect.csv", index=False)
    return values


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Kestrel M1 metrics 1, 2, 8, 9")
    p.add_argument("--curated", type=Path, default=CURATED)
    p.add_argument("--out", type=Path, default=METRICS_OUT)
    args = p.parse_args(argv)

    values = write_outputs(load_curated(args.curated), args.out)
    with pd.option_context("display.width", 200, "display.max_colwidth", 70):
        print(values.to_string(index=False))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
