import json

import pandas as pd
import pytest

from kestrel_m1 import metrics

T0 = pd.Timestamp("2026-03-01 10:00:00", tz="UTC")


def ev(session, seq, etype, at_s, latency_ms=None, late=False, tokens_in=None, tokens_out=None):
    return {"event_id": f"ev_{session}_{seq}", "event_type": etype, "sequence_number": seq, "session_id": session,
            "event_time_utc": T0 + pd.Timedelta(seconds=at_s), "lateness_s": 30.0 if late else 1.0, "is_late": late,
            "latency_ms": latency_ms, "tokens_in": tokens_in, "tokens_out": tokens_out}


@pytest.fixture
def activity():
    rows = [
        # complete success, 10 s, latency_ms agrees
        ev("ss_1", 1, "session.start", 0), ev("ss_1", 2, "llm.turn", 2), ev("ss_1", 3, "tool.call", 4),
        ev("ss_1", 4, "session.end", 10, latency_ms=10_000, late=True),
        # error with a gap at seq 3, 20 s, latency_ms disagrees
        ev("ss_2", 1, "session.start", 0), ev("ss_2", 2, "llm.turn", 3),
        ev("ss_2", 4, "session.error", 20, latency_ms=5_000),
        # no terminal row: not in the population
        ev("ss_3", 1, "session.start", 0), ev("ss_3", 2, "llm.turn", 1),
    ]
    return pd.DataFrame(rows).astype({"latency_ms": "Int64", "tokens_in": "Int64", "tokens_out": "Int64"})


def values(df):
    return df.set_index("metric").value.to_dict()


def test_terminal_population_rates_duration_and_trace(activity):
    o = metrics.session_outcomes(activity)
    assert sorted(o.session_id) == ["ss_1", "ss_2"]
    v = values(pd.concat([metrics.metric_1(o), metrics.metric_2(o), metrics.metric_8_trace(o)]))
    assert v["1 terminal sessions (volume)"] == 2
    assert v["1 success rate"] == 50.0 and v["1 error rate"] == 50.0
    assert v["2 p50 session duration"] == 15.0          # linear interpolation between 10 and 20
    assert v["2 p95 session duration"] == 19.5
    assert v["2 cross-check: terminal latency_ms disagrees > 1 s"] == 50.0
    assert v["8 trace completeness"] == 50.0


def test_dropping_late_rows_is_only_a_sensitivity(activity):
    o = metrics.session_outcomes(activity, include_late=False)
    assert list(o.session_id) == ["ss_2"]               # ss_1's terminal row was late


def test_gap_cause_is_read_from_the_rejected_slot(activity):
    o = metrics.session_outcomes(activity)
    payload = json.dumps({"session_id": "ss_2", "sequence_number": 3})
    for reason, cause in (("INVALID_NUMERIC_RANGE", "gap left by rejected rows"),
                          ("DUPLICATE_EVENT_ID", "gap in source (never sent)")):
        rejects = pd.DataFrame({"source_file": ["agent_activity.parquet"], "source_line": [7], "reason": [reason],
                                "raw_payload": [payload]})
        out = metrics.trace_breakdown(activity, o, rejects).set_index("trace_status").sessions
        assert out.to_dict() == {"complete": 1, cause: 1}


def test_correlation_coverage_variants():
    corr = pd.DataFrame({"alert_id": ["a1", "a2", "a3", "a4"],
                         "match_status": ["matched", "ambiguous", "ambiguous", "unmatched"],
                         "session_id": ["ss_1", "ss_2", None, None]})
    alerts = pd.DataFrame({"alert_id": ["a1", "a2", "a3", "a4"], "source_ip": ["10.0.0.1"] * 3 + ["203.0.113.9"]})
    flows = pd.DataFrame({"flow_id": ["nf_1"], "src_ip": ["10.0.0.1"]})
    v = values(metrics.metric_8_coverage(corr, alerts, flows))
    assert v["8 correlation coverage (alert -> session)"] == 50.0
    assert v["8 correlation coverage, strict (single-flow match only)"] == 25.0
    assert v["8 correlation coverage, on-platform alerts only"] == round(100 * 2 / 3, 3)


def test_rejected_rate_per_source_and_pooled():
    s = pd.DataFrame({"source_file": ["a", "a", "a", "b"], "outcome": ["ACCEPTED", "ORPHAN_EVENT", "DUPLICATE_EVENT_ID",
                                                                       "ACCEPTED"],
                      "rows": [90, 6, 4, 100], "share_pct": 0.0})
    v = values(metrics.metric_9_rejected(s))
    assert v["9 rejected rate: a"] == 10.0 and v["9 rejected rate: b"] == 0.0
    assert v["9 rejected rate: all four sources"] == 5.0


def test_missing_tokens_are_excluded_not_zero():
    t = pd.DataFrame({"event_type": "llm.turn", "tokens_in": pd.array([1000, 1000], "Int64"),
                      "tokens_out": pd.array([400, None], "Int64"), "input_price_per_1k": [0.01, 0.01],
                      "output_price_per_1k": [0.02, 0.02], "cost_usd": pd.array([0.018, None], "Float64")})
    e = metrics.missing_token_effect(t).set_index("quantity")
    assert e.loc["mean output_tokens per turn"].tolist()[:2] == [400.0, 200.0]
    assert e.loc["estimated LLM cost, USD"].tolist()[:2] == [0.02, 0.03]   # 0.018 vs 0.018 + 0.01 (rounded)


def test_full_pack_metrics(pack):
    o = metrics.session_outcomes(pack.act.accepted)
    v = values(pd.concat([metrics.metric_1(o), metrics.metric_2(o), metrics.metric_8_trace(o),
                          metrics.metric_9_late(pack.act.accepted)]))
    assert v["1 terminal sessions (volume)"] == 97_952
    assert v["1 error rate"] == 7.494
    assert v["2 p50 session duration"] == 17.646 and v["2 p95 session duration"] == 39.56
    assert v["8 trace completeness"] == 95.881
    assert v["9 late-event rate (lateness > 10 s)"] == 1.499
