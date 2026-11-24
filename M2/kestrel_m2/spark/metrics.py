"""The eleven required metrics (Project Description section 4, Appendix A), in Spark.

Every value is a row of ``kestrel_analytics.required_metrics`` (and
``output/metrics/m2_metrics.csv``) carrying the policy behind it: denominator,
population filter, late-event policy, null policy, and the Spark function used.
Metrics 1, 2, 8 and 9 were computed in pandas in M1; their M1 value is put next to the
Spark value, and any difference is explained in ``note``.

Percentiles: ``percentile_approx(col, p, 10000)``. Spark documents its accuracy
parameter as "1.0/accuracy is the relative error of the approximation", so the returned
value's rank is within 1/10000 = 0.01% of the requested rank, and it is always an
observed value (no interpolation). The exact ``percentile`` (linear interpolation, the
same rule as pandas) is reported next to it.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from .. import config
from . import common, detection

ACC = 10000
M1_METRICS = config.M1_DIR / "output" / "metrics" / "m1_metrics.csv"
LATE_KEPT = "late rows are accepted and flagged in batch (no watermark), so they count"
NO_LATE = "not applicable: the source has no ingest_time"


def _one(spark: SparkSession, sql: str) -> dict:
    return spark.sql(sql).collect()[0].asDict()


def _pct(n, d) -> float | None:
    return None if not d else round(100.0 * n / d, 3)


def run(spark: SparkSession, f: dict) -> pd.DataFrame:
    rows: list[dict] = []

    def add(no, metric, value, num=None, den=None, unit="%", fn="", den_policy="", population="", late="",
            null="", variant="primary", note=""):
        rows.append({"metric_no": no, "metric": metric, "variant": variant, "value": value, "numerator": num,
                     "denominator": den, "unit": unit, "spark_function": fn, "denominator_policy": den_policy,
                     "population": population, "late_policy": late, "null_policy": null, "note": note})

    # 1 ------------------------------------------------------------------------------------
    r = _one(spark, "SELECT count(*) n, sum(CASE WHEN outcome='success' THEN 1 ELSE 0 END) ok, "
                    "sum(CASE WHEN outcome='error' THEN 1 ELSE 0 END) err FROM fact_session WHERE outcome IS NOT NULL")
    pol = dict(den_policy="distinct sessions with an accepted session.end or session.error",
               population="fact_session where outcome is not null (accepted rows only, after dedup and rejects)",
               late=LATE_KEPT, null="no nulls possible: event_type/session_id are required fields")
    add(1, "session volume", r["n"], unit="sessions", fn="count(*)", **pol)
    add(1, "success rate", _pct(r["ok"], r["n"]), r["ok"], r["n"], fn="sum(case)/count", **pol)
    add(1, "error rate", _pct(r["err"], r["n"]), r["err"], r["n"], fn="sum(case)/count", **pol)

    # 2 ------------------------------------------------------------------------------------
    r = _one(spark, f"SELECT percentile_approx(duration_s, array(0.5, 0.95), {ACC}) a, "
                    f"percentile(duration_s, array(0.5, 0.95)) e, count(*) n FROM fact_session WHERE outcome IS NOT NULL")
    pol = dict(den_policy="terminal sessions (same population as metric 1)",
               population="duration = terminal event_time - session.start event_time (never latency_ms)",
               late="included: duration uses event_time only", null="sessions without a terminal row are excluded")
    for i, q in enumerate(("p50", "p95")):
        add(2, f"{q} session duration", round(r["a"][i], 3), den=r["n"], unit="s",
            fn=f"percentile_approx(duration_s, {0.5 if i == 0 else 0.95}, {ACC}); relative error 1/{ACC} = 0.01%",
            **pol)
        add(2, f"{q} session duration", round(r["e"][i], 3), den=r["n"], unit="s", variant="exact",
            fn="percentile(duration_s, p): exact, linear interpolation", **pol)

    # 3 ------------------------------------------------------------------------------------
    r = _one(spark, "SELECT count(*) n, count(is_policy_violation) j, "
                    "sum(CASE WHEN is_policy_violation THEN 1 ELSE 0 END) v FROM fact_tool_call")
    pol = dict(late=LATE_KEPT, population="accepted tool calls, judged against the policy version effective at "
                                          "the call's event_time (validity-window join)",
               null="no policy in effect (ag_019 -> sp_missing_profile): is_policy_violation is null, excluded "
                    "from the primary denominator, never counted as a violation")
    add(3, "policy-violation rate (tool calls outside the allowlist)", _pct(r["v"], r["j"]), r["v"], r["j"],
        fn="sum(case)/count(is_policy_violation)", den_policy="tool calls with a policy in effect", **pol)
    add(3, "policy-violation rate (tool calls outside the allowlist)", _pct(r["v"], r["n"]), r["v"], r["n"],
        variant="all tool calls", fn="sum(case)/count(*)", den_policy="all accepted tool calls", **pol)
    r = _one(spark, "SELECT count(*) n, count(is_policy_violation) j, "
                    "sum(CASE WHEN is_policy_violation THEN 1 ELSE 0 END) v FROM fact_network_flow")
    pol["population"] = ("accepted flows; outside the effective egress policy = no network block, denied port, "
                         "host matching no allowlist entry (wildcards matched), or IP-only destination")
    pol["late"] = NO_LATE
    add(3, "egress-violation rate", _pct(r["v"], r["j"]), r["v"], r["j"], fn="sum(case)/count(is_policy_violation)",
        den_policy="flows with a policy in effect", **pol)
    add(3, "egress-violation rate", _pct(r["v"], r["n"]), r["v"], r["n"], variant="all flows",
        fn="sum(case)/count(*)", den_policy="all accepted flows", **pol)

    # 4 ------------------------------------------------------------------------------------
    r = _one(spark, "SELECT count(*) n, sum(CASE WHEN decision='DENY' THEN 1 ELSE 0 END) d FROM fact_sandbox_decision")
    add(4, "sandbox denial rate", _pct(r["d"], r["n"]), r["d"], r["n"], fn="sum(case)/count(*)",
        den_policy="all accepted sandbox decisions", population="every audit line that was accepted (UTC-corrected)",
        late=NO_LATE, null="decision is required; '-' lines were rejected")
    r = _one(spark, "SELECT count(*) n, sum(CASE WHEN n_escalation_denied > 0 THEN 1 ELSE 0 END) e, "
                    "sum(n_escalation_denied) a FROM fact_session")
    pol = dict(den_policy="all accepted sessions", late=NO_LATE, null="sessions with no decisions count as 0",
               population="escalation class = setuid, ptrace, mount; a session counts once")
    add(4, "escalation attempts (sessions with >= 1 denied escalation-class syscall)", r["e"], unit="sessions",
        fn="count(n_escalation_denied > 0)", **pol)
    add(4, "escalation attempts (sessions with >= 1 denied escalation-class syscall)", _pct(r["e"], r["n"]), r["e"],
        r["n"], variant="share of sessions", fn="sum(case)/count(*)", **pol)
    add(4, "escalation attempts: denied escalation-class syscalls", r["a"], unit="decisions", variant="decisions",
        fn="sum(n_escalation_denied)", **pol)

    # 5 ------------------------------------------------------------------------------------
    r = _one(spark, "SELECT count(is_goal_drift) n, sum(CASE WHEN is_goal_drift THEN 1 ELSE 0 END) d FROM fact_session")
    add(5, "goal-drift rate", _pct(r["d"], r["n"]), r["d"], r["n"], fn="sum(case)/count(is_goal_drift)",
        den_policy="sessions with expected_tool_categories and >= 1 accepted tool call (Appendix A convention)",
        population="drift = the set of executed tool categories (per tool version, dim_tool) is not a subset "
                   "of the task's expected set", late=LATE_KEPT,
        null="sessions without a tool call have is_goal_drift null and are outside the denominator")
    r = _one(spark, "SELECT count(*) n, sum(CASE WHEN n_injection_indicators > 0 THEN 1 ELSE 0 END) i "
                    "FROM fact_prompt_turn")
    add(5, "injection-indicator rate", _pct(r["i"], r["n"]), r["i"], r["n"], fn="sum(case)/count(*)",
        den_policy="all accepted prompt turns", population="turns whose detector output has >= 1 indicator "
                                                           "(noisy detector, not ground truth)",
        late=LATE_KEPT, null="empty injection_indicators = no indicator")

    # 6 ------------------------------------------------------------------------------------
    r = _one(spark, "SELECT count(*) n, sum(CASE WHEN status IN ('success','failure','timeout') THEN 1 ELSE 0 END) c, "
                    "sum(CASE WHEN status IN ('failure','timeout') THEN 1 ELSE 0 END) f, "
                    "sum(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) f1, "
                    "sum(CASE WHEN retry_number > 0 THEN 1 ELSE 0 END) rt FROM fact_tool_call")
    pol = dict(late=LATE_KEPT, null="status and retry_number are required on tool calls; rows missing them were "
                                    "rejected", population="accepted tool-call attempts (each retry is an attempt)")
    add(6, "tool failure rate", _pct(r["f"], r["c"]), r["f"], r["c"], fn="sum(case)/sum(case)",
        den_policy="completed attempts = executed calls (success, failure, timeout); denied calls never ran",
        note="failed = failure + timeout", **pol)
    add(6, "tool failure rate", _pct(r["f1"], r["c"]), r["f1"], r["c"], variant="failure status only",
        fn="sum(case)/sum(case)", den_policy="completed attempts", **pol)
    add(6, "retry rate", _pct(r["rt"], r["n"]), r["rt"], r["n"], fn="sum(case)/count(*)",
        den_policy="all accepted tool-call attempts", **pol)

    # 7 ------------------------------------------------------------------------------------
    detection.rules(spark)
    r = _one(spark, "SELECT count(*) n, avg(s.n_distinct_destinations) a, "
                    f"percentile_approx(s.n_distinct_destinations, 0.5, {ACC}) m, max(s.n_distinct_destinations) x, "
                    "sum(s.n_distinct_destinations) t FROM fact_session s JOIN detection d USING (session_id) "
                    "WHERE d.r1_injection_then_violation")
    pol = dict(den_policy="rogue sessions = sessions flagged by detection rule R1 (no labels)",
               population="distinct destination = coalesce(dst_host, dst_ip) over the session's accepted flows",
               late=NO_LATE, null="a rogue session with no flows has blast radius 0")
    add(7, "blast radius (mean distinct destinations per rogue session)", round(r["a"], 3), r["t"], r["n"],
        unit="hosts", fn="avg(n_distinct_destinations)", **pol)
    add(7, "blast radius (median distinct destinations per rogue session)", r["m"], den=r["n"], unit="hosts",
        variant="median", fn=f"percentile_approx(n_distinct_destinations, 0.5, {ACC})", **pol)
    add(7, "blast radius (max distinct destinations per rogue session)", r["x"], den=r["n"], unit="hosts",
        variant="max", fn="max", **pol)
    r = _one(spark, f"""SELECT count(*) n,
            percentile_approx(cast(first_violation_after_injection_utc AS double) - cast(first_injection_time_utc AS double),
                              array(0.5, 0.95), {ACC}) q,
            percentile(cast(first_violation_after_injection_utc AS double) - cast(first_injection_time_utc AS double), 0.5) e
            FROM fact_session WHERE first_violation_after_injection_utc IS NOT NULL""")
    pol = dict(den_policy="sessions with an injection-indicator turn and a tool-policy or egress violation at or "
                          "after it", population="first indicator turn -> first violation at or after it",
               late=LATE_KEPT, null="sessions without both events are outside the population")
    add(7, "time-to-first-violation (median)", round(r["q"][0], 3), den=r["n"], unit="s",
        fn=f"percentile_approx(delta_s, 0.5, {ACC}); relative error 1/{ACC}", **pol)
    add(7, "time-to-first-violation (median)", round(r["e"], 3), den=r["n"], unit="s", variant="exact",
        fn="percentile(delta_s, 0.5)", **pol)
    add(7, "time-to-first-violation (p95)", round(r["q"][1], 3), den=r["n"], unit="s", variant="p95",
        fn=f"percentile_approx(delta_s, 0.95, {ACC})", **pol)

    # 8 ------------------------------------------------------------------------------------
    r = _one(spark, "SELECT count(*) n, sum(CASE WHEN is_trace_complete THEN 1 ELSE 0 END) c FROM fact_session "
                    "WHERE outcome IS NOT NULL")
    add(8, "trace completeness", _pct(r["c"], r["n"]), r["c"], r["n"], fn="sum(case)/count(*)",
        den_policy="terminal sessions", population="complete = >= 1 accepted llm.turn and accepted sequence_numbers "
                                                   "exactly 1..max (no gap, no repeat)",
        late="late rows are accepted and fill their slot", null="a rejected row leaves a gap")
    r = _one(spark, "SELECT count(*) n, count(session_id) s, sum(CASE WHEN match_status='matched' THEN 1 ELSE 0 END) m "
                    "FROM fact_siem_alert")
    pol = dict(den_policy="all accepted SIEM alerts", late=NO_LATE, null="an alert without a session is not covered",
               population="covered = linked to exactly one session at N = 60 s (matched, or ambiguous with all "
                          "candidate flows in one session)")
    add(8, "correlation coverage", _pct(r["s"], r["n"]), r["s"], r["n"], fn="count(session_id)/count(*)", **pol)
    add(8, "correlation coverage", _pct(r["m"], r["n"]), r["m"], r["n"], variant="strict (single-flow match)",
        fn="sum(case)/count(*)", **pol)

    # 9 ------------------------------------------------------------------------------------
    src = ("('agent_activity.parquet', 'network_flows.csv', 'sandbox_audit.log', 'siem_alerts.json')")
    rec = spark.sql(f"""SELECT c.file_name, c.row_count received, coalesce(r.n, 0) rejected
        FROM dim_source_file c LEFT JOIN (SELECT source_file, count(*) n FROM fact_rejected_record
                                          WHERE channel = 'batch' GROUP BY source_file) r ON r.source_file = c.file_name
        WHERE c.file_name IN {src}""").toPandas()
    pol = dict(den_policy="received records = accepted + rejected (catalogue record count)",
               late="no watermark in batch: lateness never rejects", null="a null required field is a reject")
    act = rec[rec.file_name == "agent_activity.parquet"].iloc[0]
    add(9, "rejected-record rate", _pct(act.rejected, act.received), int(act.rejected), int(act.received),
        fn="count(fact_rejected_record)/dim_source_file.row_count", population="agent_activity.parquet", **pol)
    add(9, "rejected-record rate", _pct(rec.rejected.sum(), rec.received.sum()), int(rec.rejected.sum()),
        int(rec.received.sum()), variant="all four event sources", fn="pooled", population="four event sources", **pol)
    r = _one(spark, "SELECT sum(n_events) n, sum(n_late_events) l FROM fact_session")
    add(9, "late-event rate", _pct(r["l"], r["n"]), r["l"], r["n"], fn="sum(n_late_events)/sum(n_events)",
        den_policy="accepted activity rows", population="late = ingest_time - event_time > 10 s",
        late="this is the late policy's measurement", null="both timestamps are required")
    try:                                                    # the stream's rates come from PostgreSQL (JDBC)
        log = common.read_pg(spark, "stream_message_log")
        cur = common.read_pg(spark, "stream_curated_event")
        recv = log.where("kafka_topic = 'kestrel.agent.events' AND outcome <> 'control'").count()
        rej = log.where("outcome = 'rejected'").count()
        acc = cur.count()
        late = cur.where("is_late").count()
        if recv:
            add(9, "rejected-record rate", _pct(rej, recv), rej, recv, variant="stream",
                fn="JDBC: stream_message_log", den_policy="data messages on the raw topic (EOS excluded)",
                population="stream_events.json via Kafka", late="beyond the 60 s watermark: LATE_BEYOND_WATERMARK",
                null="a null required field is a reject")
            add(9, "late-event rate", _pct(late, acc), late, acc, variant="stream", fn="JDBC: stream_curated_event",
                den_policy="accepted stream records", population="late = lateness > 10 s, within the watermark",
                late="records beyond the watermark are rejected, not counted here",
                null="both timestamps are required")
    except Exception as e:                                  # noqa: BLE001 - stream not run yet
        print(f"stream metrics skipped: {e}")

    # 10 -----------------------------------------------------------------------------------
    ev = detection.evaluate(spark).toPandas()
    ev.to_csv(common.SPARK_OUT / "detection_evaluation.csv", index=False)
    common.persist(spark.createDataFrame(ev), "detection_evaluation")
    for e in ev.itertuples():
        prim = e.rule == "r1_injection_then_violation" and e.label_policy == "confirmed_rogue vs benign"
        variant = "primary" if prim else f"{e.rule} / {e.label_policy}"
        pol = dict(den_policy="labelled sessions present in the warehouse; precision over flagged, recall over "
                              "positives", population=f"rule {e.rule}; labels {e.label_policy}",
                   late=LATE_KEPT, null="unlabelled sessions have no truth and are not scored")
        add(10, "detection precision", None if pd.isna(e.precision) else round(100 * e.precision, 2), e.tp,
            e.tp + e.fp, variant=variant, fn="sum(tp)/sum(flagged)", **pol)
        add(10, "detection recall", None if pd.isna(e.recall) else round(100 * e.recall, 2), e.tp, e.tp + e.fn,
            variant=variant, fn="sum(tp)/sum(positive)", **pol)

    # 11 -----------------------------------------------------------------------------------
    r = _one(spark, "SELECT count(*) n, count(cost_usd) c, round(sum(cost_usd), 2) s FROM fact_prompt_turn")
    add(11, "estimated LLM cost", float(r["s"]), r["c"], r["n"], unit="USD", fn="sum(cost_usd)",
        den_policy="prompt turns with a cost", population="(in_tokens/1000 x in_price) + (out_tokens/1000 x out_price)"
                                                          " at the price effective at turn time",
        late=LATE_KEPT, null="missing output_tokens or no effective price: cost null and excluded (never 0); "
                             "zero-imputation would add +0.79% (M1 missing_token_effect.csv)")
    by_tenant = spark.sql("SELECT tenant_id, round(sum(cost_usd), 2) c FROM fact_prompt_turn GROUP BY 1 "
                          "ORDER BY 1").collect()
    for t in by_tenant:
        add(11, "estimated LLM cost", float(t["c"]), unit="USD", variant=f"tenant {t['tenant_id']}",
            fn="sum(cost_usd) group by tenant_id", den_policy="prompt turns with a cost", population=t["tenant_id"],
            late=LATE_KEPT, null="null-excluded")

    out = pd.DataFrame(rows)
    out = compare_with_m1(out)
    (config.OUT_DIR / "metrics").mkdir(parents=True, exist_ok=True)
    out.to_csv(config.OUT_DIR / "metrics" / "m2_metrics.csv", index=False)
    common.persist(spark.createDataFrame(out.astype({"value": "float64", "numerator": "float64",
                                                     "denominator": "float64", "m1_pandas_value": "float64",
                                                     "difference": "float64"})), "required_metrics")
    return out


M1_NAME = {
    ("session volume", "primary"): "1 terminal sessions (volume)",
    ("success rate", "primary"): "1 success rate", ("error rate", "primary"): "1 error rate",
    ("p50 session duration", "primary"): "2 p50 session duration",
    ("p95 session duration", "primary"): "2 p95 session duration",
    ("p50 session duration", "exact"): "2 p50 session duration",
    ("p95 session duration", "exact"): "2 p95 session duration",
    ("trace completeness", "primary"): "8 trace completeness",
    ("correlation coverage", "primary"): "8 correlation coverage (alert -> session)",
    ("correlation coverage", "strict (single-flow match)"): "8 correlation coverage, strict (single-flow match only)",
    ("rejected-record rate", "primary"): "9 rejected rate: agent_activity.parquet",
    ("rejected-record rate", "all four event sources"): "9 rejected rate: all four sources",
    ("late-event rate", "primary"): "9 late-event rate (lateness > 10 s)",
}


def compare_with_m1(out: pd.DataFrame) -> pd.DataFrame:
    m1 = pd.read_csv(M1_METRICS).set_index("metric").value
    out["m1_pandas_value"] = [m1.get(M1_NAME.get((r.metric, r.variant)), None) for r in out.itertuples()]
    out["difference"] = (out.value.astype(float) - out.m1_pandas_value.astype(float)).round(3)
    for i, r in out.iterrows():
        if pd.isna(r.m1_pandas_value):
            continue
        if r.difference == 0:
            out.at[i, "note"] = (r.note + "; " if r.note else "") + "identical to the M1 pandas value"
        elif r.metric_no == 2 and r.variant == "primary":
            out.at[i, "note"] = ("differs from M1 by design: percentile_approx returns an observed value whose rank is "
                                 "within 1/10000 of the target, without interpolating between the two closest "
                                 "ranks as pandas does; the exact Spark percentile row equals M1")
    return out
