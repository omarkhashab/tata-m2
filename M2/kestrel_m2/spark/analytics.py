"""Lab 7 Spark: the ten analyses of Appendix C, the skew response, explain(), validation.

    python -m kestrel_m2.spark.analytics          # all analyses + metrics, persisted to PostgreSQL

=====  ======================================================  ================
id     analysis (Appendix C)                                   API
=====  ======================================================  ================
a01    daily session volume + policy-violation rate by          DataFrame
       agent/version  (multi-table join, skew response,
       the explain() analysis)
a02    approximate p50/p95 duration by agent type and hour     DataFrame
       band
a03    token, cost and turn distribution by agent/version      DataFrame
       and tenant
a04    per-session escalation chain: ordered violations with   DataFrame
       time deltas (window function, checked against a
       self-join version)
a05    trace-completeness, late-event and rejected-record      DataFrame
       rate by event type (reads the partitioned lake)
a06    top five agents by p95 duration (min session count)     SQL
a07    tool categories with the highest failure/denial rates   SQL
a08    agent versions with the largest week-over-week          SQL
       violation-rate increase
a09    sessions with the most tool attempts before their       SQL
       terminal event
a10    detection precision/recall by tenant and agent type     SQL
=====  ======================================================  ================

Skew (documented in the Dataset Description and measured in ``skew_report``): ag_004
is ~47% of activity (version 3.0.2 alone ~32%) and tn_001 ~62%. A shuffle on the agent
key sends a third of the fact table to one reducer. The explicit response:

1. **broadcast** the small dimensions in every fact-dimension join (``F.broadcast``), so
   the fact table is never shuffled on the skewed key: each task joins its own split of
   the fact against an in-memory copy of ``dim_agent``;
2. aggregate **after** the join with map-side partial aggregation, so only one partial
   row per (key, task) crosses the network;
3. where a dimension could not be broadcast, **salt** the join key (fact rows get
   ``salt = pmod(xxhash64(event_id), 8)``, the dimension is replicated once per salt):
   a01 is also computed that way, its result asserted identical, and the reducer row
   counts with and without salt are in ``skew_report``. AQE skew-join splitting stays on
   as a safety net.
"""

from __future__ import annotations

import json
import time

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .. import config
from . import common, detection

SALT = 8
PCT_ACCURACY = 10000            # percentile_approx accuracy: relative rank error 1 / 10000
AGENT_KEY = ["agent_id", "release_version"]


def ts_seconds(c: str):
    return F.col(c).cast("double")


def ratio(num, den, digits: int = 6):
    """num / den, null when den is 0 (Spark 4 runs in ANSI mode: plain / would raise)."""
    return F.round(F.try_divide(num, den), digits)


# ============================================================================ a01

def a01_daily_violation_by_agent_version(f: dict, broadcast_dims: bool = True, salted: bool = False) -> DataFrame:
    """Per UTC day and agent version: sessions started, tool calls and flows with their
    policy-violation rates. Facts are joined to dim_agent and dim_date before aggregating."""
    agent = f["dim_agent"].select(*AGENT_KEY, "agent_name", "agent_type")
    date = f["dim_date"].select("date_key", F.col("full_date").cast("date").alias("day"))
    events = (f["fact_tool_call"].select("event_id", *AGENT_KEY, "date_key", "session_id",
                                         F.lit("tool_call").alias("kind"), "is_policy_violation")
              .unionByName(f["fact_network_flow"].select(F.col("flow_id").alias("event_id"), *AGENT_KEY, "date_key",
                                                         "session_id", F.lit("network_flow").alias("kind"),
                                                         "is_policy_violation"))
              .unionByName(f["fact_session"].select(F.col("session_id").alias("event_id"), *AGENT_KEY, "date_key",
                                                    "session_id", F.lit("session").alias("kind"),
                                                    F.lit(None).cast("boolean").alias("is_policy_violation"))))
    if salted:
        events = events.withColumn("salt", F.pmod(F.xxhash64("event_id"), F.lit(SALT)))
        agent = agent.crossJoin(F.broadcast(f["dim_agent"].sparkSession.range(SALT).withColumnRenamed("id", "salt"))
                                ).withColumn("salt", F.col("salt").cast("long"))
        joined = events.join(agent, AGENT_KEY + ["salt"]).drop("salt")
    else:
        joined = events.join(F.broadcast(agent) if broadcast_dims else agent, AGENT_KEY)
    joined = joined.join(F.broadcast(date) if broadcast_dims else date, "date_key")
    judged = F.col("is_policy_violation").isNotNull()
    viol = F.coalesce(F.col("is_policy_violation"), F.lit(False))
    out = (joined.groupBy("day", *AGENT_KEY, "agent_name", "agent_type")
           .agg(F.sum(F.when(F.col("kind") == "session", 1).otherwise(0)).alias("sessions_started"),
                F.sum(F.when(F.col("kind") == "tool_call", 1).otherwise(0)).alias("tool_calls"),
                F.sum(F.when((F.col("kind") == "tool_call") & judged, 1).otherwise(0)).alias("tool_calls_judged"),
                F.sum(F.when((F.col("kind") == "tool_call") & viol, 1).otherwise(0)).alias("tool_violations"),
                F.sum(F.when((F.col("kind") == "network_flow") & judged, 1).otherwise(0)).alias("flows_judged"),
                F.sum(F.when((F.col("kind") == "network_flow") & viol, 1).otherwise(0)).alias("egress_violations")))
    return (out.withColumn("tool_violation_rate", ratio(F.col("tool_violations"), F.col("tool_calls_judged")))
            .withColumn("egress_violation_rate", ratio(F.col("egress_violations"), F.col("flows_judged")))
            .withColumn("policy_violation_rate",
                        ratio(F.col("tool_violations") + F.col("egress_violations"),
                              F.col("tool_calls_judged") + F.col("flows_judged")))
            .orderBy("day", *AGENT_KEY))


# ============================================================================ a02-a05

def a02_duration_by_agent_type_hour_band(f: dict) -> DataFrame:
    s = (f["fact_session"].where(F.col("outcome").isNotNull())
         .join(F.broadcast(f["dim_agent"].select(*AGENT_KEY, "agent_type")), AGENT_KEY)
         .join(F.broadcast(f["dim_time"].select("time_key", "hour_band")), "time_key"))
    q = F.percentile_approx("duration_s", F.array(F.lit(0.5), F.lit(0.95)), PCT_ACCURACY)
    return (s.groupBy("agent_type", "hour_band")
            .agg(F.count("*").alias("terminal_sessions"), q.alias("q"), F.round(F.avg("duration_s"), 3).alias("mean_s"))
            .select("agent_type", "hour_band", "terminal_sessions", F.round(F.col("q")[0], 3).alias("p50_duration_s"),
                    F.round(F.col("q")[1], 3).alias("p95_duration_s"), "mean_s")
            .orderBy("agent_type", "hour_band"))


def a03_tokens_cost_turns(f: dict) -> DataFrame:
    s = f["fact_session"].join(F.broadcast(f["dim_tenant"].select("tenant_id", "tenant_name")), "tenant_id")
    pct = lambda c: F.percentile_approx(c, F.array(F.lit(0.5), F.lit(0.95)), PCT_ACCURACY)   # noqa: E731
    g = (s.groupBy("tenant_id", "tenant_name", *AGENT_KEY)
         .agg(F.count("*").alias("sessions"), F.sum("n_prompt_turns").alias("prompt_turns"),
              F.round(F.avg("n_prompt_turns"), 3).alias("turns_per_session_mean"),
              pct("n_prompt_turns").alias("tq"), F.max("n_prompt_turns").alias("turns_per_session_max"),
              F.sum("tokens_in").alias("tokens_in"), F.sum("tokens_out").alias("tokens_out"),
              F.round(F.sum("cost_usd"), 6).alias("cost_usd"), pct("cost_usd").alias("cq"),
              F.sum(F.when(F.col("cost_usd").isNull(), 1).otherwise(0)).alias("sessions_without_cost")))
    return (g.select("tenant_id", "tenant_name", *AGENT_KEY, "sessions", "prompt_turns", "turns_per_session_mean",
                     F.col("tq")[0].alias("turns_per_session_p50"), F.col("tq")[1].alias("turns_per_session_p95"),
                     "turns_per_session_max", "tokens_in", "tokens_out",
                     ratio(F.col("tokens_out"), F.col("prompt_turns"), 3).alias("tokens_out_per_turn"), "cost_usd",
                     F.round(F.col("cq")[0], 6).alias("cost_per_session_p50"),
                     F.round(F.col("cq")[1], 6).alias("cost_per_session_p95"),
                     ratio(F.col("cost_usd"), F.col("sessions")).alias("cost_per_session_mean"),
                     "sessions_without_cost")
            .orderBy(F.desc("cost_usd")))


def violation_events(f: dict) -> DataFrame:
    """Every violation or escalation attempt of a session, from three facts."""
    tc = f["fact_tool_call"].where(F.col("is_policy_violation")).select(
        "session_id", F.col("event_id").alias("source_id"), F.col("event_time_utc").alias("t"),
        F.lit("tool_call").alias("activity_type"), F.lit("tool_not_allowed").alias("violation"),
        F.col("tool_id").alias("target"))
    fl = f["fact_network_flow"].where(F.col("is_policy_violation")).select(
        "session_id", F.col("flow_id").alias("source_id"), F.col("flow_time_utc").alias("t"),
        F.lit("network_flow").alias("activity_type"), F.col("violation_rule").alias("violation"),
        F.concat_ws(":", F.coalesce("dst_host", "dst_ip"), F.col("dst_port").cast("string")).alias("target"))
    sd = f["fact_sandbox_decision"].where(F.col("is_policy_violation") | F.col("is_escalation_action")).select(
        "session_id", F.col("audit_id").alias("source_id"), F.col("event_time_utc").alias("t"),
        F.lit("sandbox_decision").alias("activity_type"),
        F.concat_ws(":", F.lower("decision"), "action", F.lower("reason")).alias("violation"),
        F.col("target"))
    return tc.unionByName(fl).unionByName(sd)


def a04_escalation_chain(f: dict) -> DataFrame:
    """Ordered violations per session with the time since the previous one (window functions)."""
    w = Window.partitionBy("session_id").orderBy("t", "source_id")
    v = violation_events(f)
    return (v.withColumn("step", F.row_number().over(w))
            .withColumn("prev_t", F.lag("t").over(w))
            .withColumn("delta_s", F.round(ts_seconds("t") - ts_seconds("prev_t"), 3))
            .withColumn("since_first_s", F.round(ts_seconds("t") - F.min(ts_seconds("t")).over(
                Window.partitionBy("session_id")), 3))
            .withColumn("chain_length", F.count("*").over(Window.partitionBy("session_id")))
            .select("session_id", "step", "chain_length", F.col("t").alias("event_time_utc"), "activity_type",
                    "violation", "target", "delta_s", "since_first_s", "source_id")
            .orderBy("session_id", "step"))


def a04_without_window(f: dict) -> DataFrame:
    """The same chain with no window function: a self-join counts the earlier violations
    and finds the previous one's time, a group-by gives the first time and the length."""
    v = violation_events(f)
    a, b = v.alias("a"), v.alias("b")
    earlier = (F.col("b.t") < F.col("a.t")) | ((F.col("b.t") == F.col("a.t")) & (F.col("b.source_id") < F.col("a.source_id")))
    prev = (a.join(b, (F.col("a.session_id") == F.col("b.session_id")) & earlier, "left")
            .groupBy("a.session_id", "a.source_id", "a.t", "a.activity_type", "a.violation", "a.target")
            .agg(F.count("b.source_id").alias("n_before"), F.max("b.t").alias("prev_t")))
    per_session = v.groupBy("session_id").agg(F.min("t").alias("first_t"), F.count("*").alias("chain_length"))
    return (prev.join(per_session, "session_id")
            .select("session_id", (F.col("n_before") + 1).cast("int").alias("step"), "chain_length",
                    F.col("t").alias("event_time_utc"), "activity_type", "violation", "target",
                    F.round(ts_seconds("t") - ts_seconds("prev_t"), 3).alias("delta_s"),
                    F.round(ts_seconds("t") - ts_seconds("first_t"), 3).alias("since_first_s"), "source_id"))


def a05_quality_by_event_type(f: dict) -> DataFrame:
    """Per activity event type: received, rejected, late and trace-complete rates.

    Reads the partitioned lake (``canonical_activity``) and joins it to fact_session for
    trace completeness, and to the rejects (event_type parsed from the raw payload)."""
    acc = (f["canonical_activity"].where(F.col("source_file") == "agent_activity.parquet")
           .select(F.col("activity_subtype").alias("event_type"), "session_id", "lateness_s")
           .join(F.broadcast(f["fact_session"].select("session_id", "is_trace_complete")), "session_id", "left")
           .groupBy("event_type")
           .agg(F.count("*").alias("accepted"), F.sum(F.when(F.col("lateness_s") > 10, 1).otherwise(0)).alias("late"),
                F.sum(F.when(F.col("is_trace_complete"), 1).otherwise(0)).alias("in_complete_trace")))
    rej = (f["fact_rejected_record"].where((F.col("source_file") == "agent_activity.parquet")
                                           & (F.col("channel") == "batch"))
           .select(F.coalesce(F.get_json_object("raw_payload", "$.event_type"), F.lit("<null>")).alias("event_type"))
           .groupBy("event_type").agg(F.count("*").alias("rejected")))
    out = acc.join(rej, "event_type", "full").fillna(0, ["accepted", "late", "in_complete_trace", "rejected"])
    return (out.withColumn("received", F.col("accepted") + F.col("rejected"))
            .select("event_type", "received", "accepted", "rejected",
                    ratio(F.col("rejected"), F.col("received")).alias("rejected_rate"), "late",
                    ratio(F.col("late"), F.col("accepted")).alias("late_rate"), "in_complete_trace",
                    ratio(F.col("in_complete_trace"), F.col("accepted")).alias("trace_complete_share"))
            .orderBy(F.desc("received")))


# ============================================================================ SQL a06-a10

A06 = f"""
SELECT s.agent_id, first(a.agent_name) AS agent_name, first(a.agent_type) AS agent_type,
       count(*) AS terminal_sessions,
       round(percentile_approx(s.duration_s, 0.5, {PCT_ACCURACY}), 3)  AS p50_duration_s,
       round(percentile_approx(s.duration_s, 0.95, {PCT_ACCURACY}), 3) AS p95_duration_s,
       round(max(s.duration_s), 3) AS max_duration_s
FROM fact_session s
JOIN dim_agent a ON a.agent_id = s.agent_id AND a.release_version = s.release_version
WHERE s.outcome IS NOT NULL
GROUP BY s.agent_id
HAVING count(*) >= 500
ORDER BY p95_duration_s DESC, s.agent_id
LIMIT 5
"""

A07 = """
SELECT t.tool_category,
       count(*)                                                                    AS attempts,
       sum(CASE WHEN c.status IN ('success', 'failure', 'timeout') THEN 1 ELSE 0 END) AS executed,
       sum(CASE WHEN c.status IN ('failure', 'timeout') THEN 1 ELSE 0 END)           AS failed,
       sum(CASE WHEN c.status = 'denied' THEN 1 ELSE 0 END)                          AS denied,
       round(sum(CASE WHEN c.status IN ('failure', 'timeout') THEN 1 ELSE 0 END)
             / nullif(sum(CASE WHEN c.status IN ('success', 'failure', 'timeout') THEN 1 ELSE 0 END), 0), 6)
                                                                                     AS failure_rate,
       round(sum(CASE WHEN c.status = 'denied' THEN 1 ELSE 0 END) / count(*), 6)      AS denial_rate,
       round(sum(CASE WHEN c.is_policy_violation THEN 1 ELSE 0 END)
             / nullif(count(c.is_policy_violation), 0), 6)                            AS policy_violation_rate,
       count(DISTINCT c.tool_id)                                                     AS tools
FROM fact_tool_call c
JOIN dim_tool t ON t.tool_id = c.tool_id AND t.tool_version = c.tool_version
GROUP BY t.tool_category
ORDER BY failure_rate DESC, denial_rate DESC
"""

A08 = """
WITH weekly AS (
    SELECT c.agent_id, c.release_version, d.iso_year, d.iso_week, min(d.full_date) AS week_first_day,
           count(*) AS judged_calls,
           sum(CASE WHEN c.is_policy_violation THEN 1 ELSE 0 END) AS violations
    FROM fact_tool_call c JOIN dim_date d ON d.date_key = c.date_key
    WHERE c.is_policy_violation IS NOT NULL
    GROUP BY c.agent_id, c.release_version, d.iso_year, d.iso_week
), rated AS (
    SELECT *, violations / judged_calls AS violation_rate,
           lag(violations / judged_calls) OVER (PARTITION BY agent_id, release_version ORDER BY iso_year, iso_week)
               AS prev_violation_rate,
           lag(judged_calls) OVER (PARTITION BY agent_id, release_version ORDER BY iso_year, iso_week) AS prev_calls,
           lag(iso_week) OVER (PARTITION BY agent_id, release_version ORDER BY iso_year, iso_week) AS prev_week
    FROM weekly
)
SELECT agent_id, release_version, iso_year, iso_week, cast(week_first_day AS date) AS week_first_day, prev_week,
       prev_calls, judged_calls, round(prev_violation_rate, 6) AS prev_violation_rate,
       round(violation_rate, 6) AS violation_rate,
       round(violation_rate - prev_violation_rate, 6) AS wow_increase
FROM rated
WHERE prev_violation_rate IS NOT NULL AND iso_week = prev_week + 1
  AND judged_calls >= 50 AND prev_calls >= 50
ORDER BY wow_increase DESC, agent_id, release_version
LIMIT 10
"""

A09 = """
SELECT s.session_id, s.tenant_id, s.agent_id, s.release_version, s.outcome, s.terminal_event_type,
       count(c.event_id)                                            AS tool_attempts_before_terminal,
       sum(CASE WHEN c.retry_number > 0 THEN 1 ELSE 0 END)          AS retries,
       sum(CASE WHEN c.status IN ('failure', 'timeout') THEN 1 ELSE 0 END) AS failed,
       sum(CASE WHEN c.status = 'denied' THEN 1 ELSE 0 END)         AS denied,
       first(a.max_tool_calls_per_session)                          AS declared_budget,
       count(c.event_id) > first(a.max_tool_calls_per_session)      AS over_budget,
       round(first(s.duration_s), 3)                                AS duration_s
FROM fact_session s
JOIN fact_tool_call c ON c.session_id = s.session_id AND c.event_time_utc < s.end_time_utc
JOIN dim_agent a ON a.agent_id = s.agent_id AND a.release_version = s.release_version
WHERE s.outcome IS NOT NULL
GROUP BY s.session_id, s.tenant_id, s.agent_id, s.release_version, s.outcome, s.terminal_event_type
ORDER BY tool_attempts_before_terminal DESC, s.session_id
LIMIT 20
"""


def a10_detection_by_tenant_agent_type(spark: SparkSession) -> DataFrame:
    """Detection precision/recall by tenant and agent type (labels read once, here)."""
    return detection.evaluate(spark, by=["tenant_id", "agent_type"]).orderBy("rule", "label_policy", "tenant_id",
                                                                               "agent_type")


# ============================================================================ skew

def skew_report(spark: SparkSession, f: dict) -> pd.DataFrame:
    """Where the data concentrates, and what a shuffle on the agent key does to reducers."""
    rows = []
    facts = {"fact_tool_call": f["fact_tool_call"], "fact_prompt_turn": f["fact_prompt_turn"],
             "fact_session": f["fact_session"]}
    for name, df in facts.items():
        total = df.count()
        for key in (["agent_id", "release_version"], ["tenant_id"]):
            top = df.groupBy(*key).count().orderBy(F.desc("count")).limit(3).collect()
            for rank, r in enumerate(top, 1):
                rows.append({"section": "concentration", "table": name, "key": "+".join(key), "rank": rank,
                             "value": "@".join(str(r[k]) for k in key), "rows": r["count"],
                             "share_pct": round(100 * r["count"] / total, 2)})
    tc = f["fact_tool_call"].select("event_id", *AGENT_KEY).withColumn("salt", F.pmod(F.xxhash64("event_id"), F.lit(SALT)))
    n = int(spark.conf.get("spark.sql.shuffle.partitions"))
    for label, cols in (("hash(agent_id, release_version)", AGENT_KEY),
                        (f"hash(agent_id, release_version, salt) salt={SALT}", AGENT_KEY + ["salt"])):
        sizes = (tc.repartition(n, *cols).groupBy(F.spark_partition_id().alias("p")).count().toPandas()
                 .set_index("p")["count"].reindex(range(n), fill_value=0))
        rows.append({"section": "reducer rows (fact_tool_call, 16 shuffle partitions)", "table": "fact_tool_call",
                     "key": label, "rank": None, "value": f"max {sizes.max():,} / median {int(sizes.median()):,} / "
                                                          f"min {sizes.min():,}; empty {int((sizes == 0).sum())}",
                     "rows": int(sizes.max()), "share_pct": round(100 * sizes.max() / sizes.sum(), 2)})
    return pd.DataFrame(rows)


def explain_text(df: DataFrame, mode: str = "formatted") -> str:
    return df._jdf.queryExecution().explainString(df._sc._jvm.org.apache.spark.sql.execution.ExplainMode.fromString(mode))


def final_plan(df: DataFrame) -> str:
    """The AQE plan actually executed (call after an action)."""
    return df._jdf.queryExecution().executedPlan().toString()


# ============================================================================ validation

def validate_a01_pandas(a01: pd.DataFrame, n: int = 25, seed: int = 7) -> pd.DataFrame:
    """Independent recomputation of a01 for a random sample of (day, agent, version) keys
    with pandas, straight from the Parquet facts (no Spark)."""
    p = common.PARQUET
    sample = a01.sample(n=min(n, len(a01)), random_state=seed)
    keys = set(zip(pd.to_datetime(sample.day).dt.strftime("%Y%m%d").astype(int), sample.agent_id,
                   sample.release_version))
    tc = pd.read_parquet(p / "fact_tool_call.parquet", columns=["date_key", *AGENT_KEY, "is_policy_violation"])
    fl = pd.read_parquet(p / "fact_network_flow.parquet", columns=["date_key", *AGENT_KEY, "is_policy_violation"])
    fs = pd.read_parquet(p / "fact_session.parquet", columns=["date_key", *AGENT_KEY])
    sel = lambda df: df[[k in keys for k in zip(df.date_key, df.agent_id, df.release_version)]]   # noqa: E731
    g = ["date_key", *AGENT_KEY]
    tcs, fls = sel(tc), sel(fl)
    ind = pd.DataFrame({
        "sessions_started": sel(fs).groupby(g).size(),
        "tool_calls": tcs.groupby(g).size(),
        "tool_calls_judged": tcs.groupby(g).is_policy_violation.count(),
        "tool_violations": tcs.groupby(g).is_policy_violation.sum(),
        "flows_judged": fls.groupby(g).is_policy_violation.count(),
        "egress_violations": fls.groupby(g).is_policy_violation.sum()}).fillna(0).astype("int64").reset_index()
    sp = sample.assign(date_key=pd.to_datetime(sample.day).dt.strftime("%Y%m%d").astype(int))
    m = sp.merge(ind, on=g, how="left", suffixes=("_spark", "_pandas"))
    cols = ["sessions_started", "tool_calls", "tool_calls_judged", "tool_violations", "flows_judged",
            "egress_violations"]
    m["equal"] = pd.concat([m[f"{c}_spark"].astype("int64") == m[f"{c}_pandas"].fillna(0).astype("int64")
                            for c in cols], axis=1).all(axis=1)
    return m[["day", *AGENT_KEY] + [f"{c}_{s}" for c in cols for s in ("spark", "pandas")] + ["equal"]]


def validate_a07_postgres(a07: pd.DataFrame, n: int = 4, seed: int = 11) -> pd.DataFrame:
    """Independent recomputation of a07 for a sample of tool categories in PostgreSQL SQL,
    against the loaded warehouse (another engine, another copy of the data)."""
    import psycopg2
    cats = a07.tool_category.sample(n=min(n, len(a07)), random_state=seed).tolist()
    conn = psycopg2.connect(config.psycopg_dsn())
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT t.tool_category, count(*),
                   count(*) FILTER (WHERE c.status IN ('failure', 'timeout')),
                   count(*) FILTER (WHERE c.status = 'denied'),
                   round(count(*) FILTER (WHERE c.status IN ('failure', 'timeout'))::numeric
                         / count(*) FILTER (WHERE c.status IN ('success', 'failure', 'timeout')), 6),
                   round(count(*) FILTER (WHERE c.status = 'denied')::numeric / count(*), 6)
            FROM {config.DW_SCHEMA}.fact_tool_call c
            JOIN {config.DW_SCHEMA}.dim_tool t USING (tool_id, tool_version)
            WHERE t.tool_category = ANY(%s) GROUP BY 1""", (cats,))
        pg = pd.DataFrame(cur.fetchall(), columns=["tool_category", "attempts", "failed", "denied", "failure_rate",
                                                   "denial_rate"])
    conn.close()
    m = a07[a07.tool_category.isin(cats)].merge(pg, on="tool_category", suffixes=("_spark", "_postgres"))
    m["equal"] = ((m.attempts_spark == m.attempts_postgres) & (m.failed_spark == m.failed_postgres)
                  & (m.denied_spark == m.denied_postgres)
                  & ((m.failure_rate_spark - m.failure_rate_postgres.astype(float)).abs() < 1e-9)
                  & ((m.denial_rate_spark - m.denial_rate_postgres.astype(float)).abs() < 1e-9))
    return m


def validate_a06_exact(a06: pd.DataFrame) -> pd.DataFrame:
    """a06 uses percentile_approx; check each p95 against numpy's exact percentile."""
    import numpy as np
    s = pd.read_parquet(common.PARQUET / "fact_session.parquet", columns=["agent_id", "outcome", "duration_s"])
    s = s[s.outcome.notna()]
    rows = []
    for r in a06.itertuples():
        d = np.sort(s.loc[s.agent_id == r.agent_id, "duration_s"].astype(float).to_numpy())
        exact = float(np.percentile(d, 95))
        rank = np.searchsorted(d, r.p95_duration_s, side="right") / len(d)    # where the approx value sits
        rows.append({"agent_id": r.agent_id, "sessions": len(d), "p95_spark_approx": r.p95_duration_s,
                     "p95_numpy_exact": round(exact, 3), "abs_diff_s": round(abs(exact - r.p95_duration_s), 3),
                     "approx_value_rank": round(rank, 5),
                     "within_rank_error": abs(rank - 0.95) <= 1 / PCT_ACCURACY + 1 / len(d)})
    return pd.DataFrame(rows)


# ============================================================================ main

def run() -> dict:
    t0 = time.time()
    log = lambda m: print(f"[{time.time() - t0:6.1f}s] {m}", flush=True)   # noqa: E731
    out = common.SPARK_OUT
    out.mkdir(parents=True, exist_ok=True)
    common.ensure_analytics_schema()
    spark = common.session()
    f = common.register_star(spark)
    detection.rules(spark)
    log("spark session up, star tables + lake registered")

    results: dict[str, pd.DataFrame] = {}

    def save(name: str, df: DataFrame) -> pd.DataFrame:
        n = common.persist(df, name)
        results[name] = pd.read_csv(out / "analyses" / f"{name}.csv")
        log(f"{name}: {n:,} rows -> {config.ANALYTICS_SCHEMA}.{name}")
        return results[name]

    # ---- a01 with the explain() study: baseline (no broadcast, no AQE) vs the skew response
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    base = a01_daily_violation_by_agent_version(f, broadcast_dims=False)
    (out / "explain_a01_baseline.txt").write_text(explain_text(base), encoding="utf-8")
    tb = time.time()
    base_pdf = base.toPandas()
    base_s = time.time() - tb
    salted = a01_daily_violation_by_agent_version(f, broadcast_dims=False, salted=True)   # still no broadcast
    (out / "explain_a01_salted_join.txt").write_text(explain_text(salted), encoding="utf-8")
    salted_pdf = salted.toPandas()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    a01 = a01_daily_violation_by_agent_version(f, broadcast_dims=True)
    (out / "explain_a01_skew_response.txt").write_text(explain_text(a01), encoding="utf-8")
    tr = time.time()
    resp_pdf = a01.toPandas()
    resp_s = time.time() - tr
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    same = (base_pdf.equals(resp_pdf) and resp_pdf.sort_values(["day", *AGENT_KEY]).reset_index(drop=True)
            .equals(salted_pdf.sort_values(["day", *AGENT_KEY]).reset_index(drop=True)))
    a01_pdf = save("a01_daily_violation_by_agent_version", a01)
    (out / "explain_a01_aqe_final.txt").write_text(final_plan(a01), encoding="utf-8")
    skew = skew_report(spark, f)
    skew = pd.concat([skew, pd.DataFrame([
        {"section": "a01 wall time (local[*], AQE off)", "table": "a01", "key": "baseline: shuffle facts on agent key",
         "rank": None, "value": f"{base_s:.2f} s", "rows": None, "share_pct": None},
        {"section": "a01 wall time (local[*], AQE off)", "table": "a01", "key": "skew response: broadcast dims",
         "rank": None, "value": f"{resp_s:.2f} s", "rows": None, "share_pct": None},
        {"section": "a01 result check", "table": "a01", "key": "baseline == broadcast == salted join", "rank": None,
         "value": str(same), "rows": len(resp_pdf), "share_pct": None}])], ignore_index=True)
    skew.to_csv(out / "skew_report.csv", index=False)
    if not same:
        raise SystemExit("a01: the three join strategies disagree")
    log(f"a01 explain + skew study done (baseline {base_s:.2f}s, broadcast {resp_s:.2f}s, identical={same})")

    save("a02_duration_by_agent_type_hour_band", a02_duration_by_agent_type_hour_band(f))
    save("a03_tokens_cost_turns_by_agent_tenant", a03_tokens_cost_turns(f))
    a04 = a04_escalation_chain(f)
    save("a04_session_escalation_chain", a04)
    nowin = a04_without_window(f)
    cols = a04.columns
    diff1 = a04.select(*cols).exceptAll(nowin.select(*cols)).count()
    diff2 = nowin.select(*cols).exceptAll(a04.select(*cols)).count()
    window_check = {"rows_window": a04.count(), "rows_self_join": nowin.count(), "in_window_not_self_join": diff1,
                    "in_self_join_not_window": diff2, "identical": diff1 == 0 and diff2 == 0,
                    "window_plan_operators": [l.strip() for l in explain_text(a04, "simple").splitlines()
                                              if "Window" in l][:3]}
    (out / "window_function_check.json").write_text(json.dumps(window_check, indent=2))
    log(f"a04 window vs self-join: {window_check['identical']}")
    save("a05_quality_rates_by_event_type", a05_quality_by_event_type(f))
    save("a06_top5_agents_p95_duration", spark.sql(A06))
    save("a07_tool_category_failure_denial", spark.sql(A07))
    save("a08_agent_version_wow_violation_increase", spark.sql(A08))
    save("a09_sessions_most_tool_attempts", spark.sql(A09))
    save("a10_detection_by_tenant_agent_type", a10_detection_by_tenant_agent_type(spark))

    v1 = validate_a01_pandas(a01_pdf)
    v2 = validate_a07_postgres(results["a07_tool_category_failure_denial"])
    v3 = validate_a06_exact(results["a06_top5_agents_p95_duration"])
    v1.to_csv(out / "validation_a01_vs_pandas.csv", index=False)
    v2.to_csv(out / "validation_a07_vs_postgres.csv", index=False)
    v3.to_csv(out / "validation_a06_vs_numpy_exact.csv", index=False)
    log(f"validation: a01 {v1.equal.sum()}/{len(v1)} keys equal (pandas), a07 {v2.equal.sum()}/{len(v2)} categories "
        f"equal (PostgreSQL), a06 {v3.within_rank_error.sum()}/{len(v3)} p95 within the stated error (numpy)")

    from . import metrics
    m = metrics.run(spark, f)
    log(f"required metrics: {len(m)} rows -> {config.ANALYTICS_SCHEMA}.required_metrics")
    spark.stop()
    ok = bool(v1.equal.all() and v2.equal.all() and v3.within_rank_error.all() and window_check["identical"])
    log("done" if ok else "done WITH VALIDATION FAILURES")
    return {"ok": ok}


def main() -> None:
    res = run()
    raise SystemExit(0 if res["ok"] else 1)


if __name__ == "__main__":
    main()
