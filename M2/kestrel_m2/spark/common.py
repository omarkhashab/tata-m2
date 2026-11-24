"""Spark session, inputs and outputs shared by the analyses and the metrics.

Spark reads the warehouse's **Parquet mirror** (``output/warehouse/parquet``: the same
rows that were loaded into PostgreSQL, written by the same build), the partitioned lake
(``output/lake/canonical_activity``) and, over JDBC, the streaming tables that only live
in PostgreSQL. Results go back to PostgreSQL over JDBC (schema ``kestrel_analytics``)
and, for review, to ``output/spark/analyses/*.csv``.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession

from .. import config

PARQUET = config.OUT_DIR / "warehouse" / "parquet"
LAKE = config.OUT_DIR / "lake" / "canonical_activity"
SPARK_OUT = config.OUT_DIR / "spark"
PG_JDBC = "org.postgresql:postgresql:42.7.4"

STAR_TABLES = ["dim_tenant", "dim_agent", "dim_tool", "dim_policy", "dim_date", "dim_time", "fact_session",
               "fact_prompt_turn", "fact_tool_call", "fact_network_flow", "fact_sandbox_decision", "fact_siem_alert",
               "fact_rejected_record", "dim_source_file"]


def session(app: str = "kestrel-m2") -> SparkSession:
    b = (SparkSession.builder.appName(app)
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.skewJoin.enabled", "true")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED"))
    # the PostgreSQL JDBC driver: a local jar on the driver class path (local mode runs the
    # executors in the driver JVM); the Docker image ships it in /opt/jars. Without one,
    # Spark resolves it from Maven Central (needs winutils on Windows).
    jar = os.environ.get("KESTREL_SPARK_JDBC_JAR")
    b = b.config("spark.driver.extraClassPath", jar) if jar else b.config("spark.jars.packages", PG_JDBC)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def register_star(spark: SparkSession) -> dict[str, DataFrame]:
    """Every star table as a DataFrame and as a temp view of the same name."""
    frames = {}
    for t in STAR_TABLES:
        df = spark.read.parquet(str(PARQUET / f"{t}.parquet"))
        df.createOrReplaceTempView(t)
        frames[t] = df
    lake = spark.read.parquet(str(LAKE))                  # hive partition column event_date is discovered
    lake.createOrReplaceTempView("canonical_activity")
    frames["canonical_activity"] = lake
    return frames


def read_pg(spark: SparkSession, table: str) -> DataFrame:
    url, props = config.jdbc_url()
    return spark.read.jdbc(url, f"{config.DW_SCHEMA}.{table}", properties=props)


def ensure_analytics_schema() -> None:
    import psycopg2
    conn = psycopg2.connect(config.psycopg_dsn())
    with conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {config.ANALYTICS_SCHEMA}")
    conn.close()


def persist(df: DataFrame, name: str) -> int:
    """Replace ``kestrel_analytics.<name>`` with ``df`` (JDBC overwrite) and save a CSV copy."""
    url, props = config.jdbc_url()
    df.write.jdbc(url, f"{config.ANALYTICS_SCHEMA}.{name}", mode="overwrite", properties=props)
    pdf = df.toPandas()
    out = SPARK_OUT / "analyses"
    out.mkdir(parents=True, exist_ok=True)
    pdf.to_csv(out / f"{name}.csv", index=False)
    return len(pdf)
