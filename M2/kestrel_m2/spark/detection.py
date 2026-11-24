"""Detection rules (no labels) and their one-time evaluation against incident_labels.csv.

The rules read only observable signals from ``fact_session``. They were fixed from the
scenario in the Project Description before the labels were opened, and the labels are
read in exactly one place (``evaluate``), at the end, to score them. No rule, feature or
threshold reads a label.

* **R1 injection -> violation** (primary): the session has a prompt turn carrying an
  injection indicator, and a tool-policy or egress violation at or after that turn. This
  is the scenario's own chain: suspicious content in the context, then the agent acts
  outside its sandbox policy.
* **R2 injection -> violation + goal drift** (stricter): R1, and the executed tool
  categories leave the task's expected set.

Scoring policy: positive label = ``confirmed_rogue``, negative = ``benign``.
``suspicious`` sessions are uncertain and excluded from the primary score; a variant that
counts them as positive is reported too. Only labelled sessions that exist in the
warehouse can be scored (unlabelled sessions have no truth, so precision is measured on
the labelled sample).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config

RULES_SQL = """
SELECT s.session_id, s.tenant_id, s.agent_id, s.release_version, a.agent_type,
       s.first_violation_after_injection_utc IS NOT NULL                          AS r1_injection_then_violation,
       s.first_violation_after_injection_utc IS NOT NULL AND coalesce(s.is_goal_drift, false)
                                                                                    AS r2_injection_violation_drift
FROM fact_session s
JOIN dim_agent a ON a.agent_id = s.agent_id AND a.release_version = s.release_version
"""


def rules(spark: SparkSession) -> DataFrame:
    df = spark.sql(RULES_SQL)
    df.createOrReplaceTempView("detection")
    return df


def read_labels(spark: SparkSession) -> DataFrame:
    """The held-out ground truth. Called once, by ``evaluate``."""
    labels = (spark.read.option("header", True).csv(str(config.RAW_DIR / "incident_labels.csv"))
              .select("session_id", "label"))
    labels.createOrReplaceTempView("incident_labels")
    return labels


EVAL_SQL = """
WITH scored AS (
    SELECT d.*, l.label,
           CASE WHEN l.label = 'confirmed_rogue' THEN 1 WHEN l.label = 'benign' THEN 0 END AS y_strict,
           CASE WHEN l.label IN ('confirmed_rogue', 'suspicious') THEN 1 ELSE 0 END          AS y_broad
    FROM detection d JOIN incident_labels l USING (session_id)
)
SELECT {group_cols}
       '{rule}'                                                                   AS rule,
       '{variant}'                                                                AS label_policy,
       sum(CASE WHEN {pred} AND {y} = 1 THEN 1 ELSE 0 END)                        AS tp,
       sum(CASE WHEN {pred} AND {y} = 0 THEN 1 ELSE 0 END)                        AS fp,
       sum(CASE WHEN NOT {pred} AND {y} = 1 THEN 1 ELSE 0 END)                    AS fn,
       sum(CASE WHEN NOT {pred} AND {y} = 0 THEN 1 ELSE 0 END)                    AS tn,
       round(sum(CASE WHEN {pred} AND {y} = 1 THEN 1 ELSE 0 END)
             / nullif(sum(CASE WHEN {pred} THEN 1 ELSE 0 END), 0), 4)             AS precision,
       round(sum(CASE WHEN {pred} AND {y} = 1 THEN 1 ELSE 0 END)
             / nullif(sum({y}), 0), 4)                                            AS recall
FROM scored WHERE {y} IS NOT NULL
{group_by}
"""


def evaluate(spark: SparkSession, by: list[str] | None = None) -> DataFrame:
    """Precision / recall of every rule under both label policies (optionally grouped)."""
    if "incident_labels" not in [t.name for t in spark.catalog.listTables()]:
        read_labels(spark)
    group_cols = "".join(f"{c}, " for c in (by or []))
    group_by = f"GROUP BY {', '.join(by)}" if by else ""
    parts = []
    for rule in ("r1_injection_then_violation", "r2_injection_violation_drift"):
        for variant, y in (("confirmed_rogue vs benign", "y_strict"), ("confirmed_rogue+suspicious vs benign", "y_broad")):
            parts.append(spark.sql(EVAL_SQL.format(group_cols=group_cols, rule=rule, variant=variant, pred=rule, y=y,
                                                   group_by=group_by)))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.withColumn("f1", F.round(F.try_divide(2 * F.col("precision") * F.col("recall"),
                                                     F.col("precision") + F.col("recall")), 4))
