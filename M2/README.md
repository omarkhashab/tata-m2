# Kestrel · Milestone 2: Contracts, Warehouse, Storage, Kafka, Spark

GUC CSEN1095 Data Engineering, Winter 2026. This milestone builds pipeline stages 3 to 5 (model and load, stream,
analyse) on top of the Milestone 1 cleaning package (`../M1/kestrel_m1`), which it imports unchanged. The raw pack is
read from `../M1/data/raw`.

| Deliverable | Where |
|---|---|
| Pydantic models and Pandera schemas, with a demonstrated failing run | [`kestrel_m2/contracts.py`](kestrel_m2/contracts.py), [`gate.py`](kestrel_m2/gate.py), [`schemas.py`](kestrel_m2/schemas.py) · failing run: [`output/contracts/broken_input_demo.md`](output/contracts/broken_input_demo.md) · gate vs M1: [`reader_gate_summary.csv`](output/contracts/reader_gate_summary.csv) |
| PostgreSQL star schema with versioned `dim_agent` and `dim_policy`, and the run-twice idempotency test | [`kestrel_m2/star.py`](kestrel_m2/star.py) (spec + builder), [`warehouse.py`](kestrel_m2/warehouse.py) (load), [`sql/ddl.sql`](kestrel_m2/sql/ddl.sql), [`sql/aggregates.sql`](kestrel_m2/sql/aggregates.sql) · test: [`tests/test_idempotent_load.py`](tests/test_idempotent_load.py) + full-size [`output/warehouse/idempotency_check.csv`](output/warehouse/idempotency_check.csv) |
| Spark `explain()` output for one analysis, with the plan explained | [`output/spark/explain_a01_*.txt`](output/spark/), [`docs/spark_plan.md`](docs/spark_plan.md) |
| Partitioned Parquet, the CSV benchmark, and DuckDB queries | [`output/lake/canonical_activity/`](output/lake/canonical_activity/) · [`output/storage/`](output/storage/) (`benchmark.csv`, `layouts.csv`, `partition_key_file_counts.csv`, `duckdb_queries.md`) · [`kestrel_m2/storage.py`](kestrel_m2/storage.py) |
| Source-file catalogue | table `kestrel_dw.dim_source_file`, [`output/warehouse/source_file_catalog.csv`](output/warehouse/source_file_catalog.csv), [`kestrel_m2/catalog.py`](kestrel_m2/catalog.py) · policies flattened in DuckDB: [`policies_duckdb.py`](kestrel_m2/policies_duckdb.py) |
| Kafka producer and consumer, with the late-start test | [`kestrel_m2/stream/producer.py`](kestrel_m2/stream/producer.py), [`consumer.py`](kestrel_m2/stream/consumer.py), [`late_start_test.py`](kestrel_m2/stream/late_start_test.py) · [`output/stream/late_start_test.md`](output/stream/late_start_test.md) |
| Bonus: watermark, windowed aggregation, Spark window function | watermark in the consumer, caught record [`output/stream/watermark_caught_example.json`](output/stream/watermark_caught_example.json) · windows [`stream/windows.py`](kestrel_m2/stream/windows.py), [`tenant_minute_windows.csv`](output/stream/tenant_minute_windows.csv) · Spark window: a04, [`window_function_check.json`](output/spark/window_function_check.json) |
| Ten Spark analyses persisted as tables; two validated independently | [`kestrel_m2/spark/analytics.py`](kestrel_m2/spark/analytics.py) → schema `kestrel_analytics`, CSV copies in [`output/spark/analyses/`](output/spark/analyses/) · validations: [`output/spark/validation_*.csv`](output/spark/) |
| All eleven required metrics, each with the policy behind it | [`kestrel_m2/spark/metrics.py`](kestrel_m2/spark/metrics.py) → [`output/metrics/m2_metrics.csv`](output/metrics/m2_metrics.csv), `kestrel_analytics.required_metrics` |
| Updated data dictionary | [`docs/data_dictionary.md`](docs/data_dictionary.md), [`docs/warehouse_columns.md`](docs/warehouse_columns.md), [`docs/spark_plan.md`](docs/spark_plan.md) |

---

## 1. Run it from a clean clone

**Prerequisites:** Git and Docker (Docker Desktop on Windows/macOS) with Compose v2, at least about 6 GB of memory for
Docker. Ports 5432, 8080, 8081 and 9092 must be free; stop the M1 stack first (`cd ../M1 && docker compose down`).

```bash
git clone <repository-url> kestrel
cd kestrel/M2
docker compose up --build
```

The one-shot jobs run in dependency order, about 10 minutes in total. The image builds once for all of them.

| Service | What it does | Ends |
|---|---|---|
| `postgres`, `adminer` (<http://localhost:8080>) | PostgreSQL 16 (`kestrel` / `kestrel` / `kestrel`) and its web UI | keeps running |
| `zookeeper`, `kafka`, `kafka-ui` (<http://localhost:8081>) | the broker and a topic browser | keeps running |
| `warehouse` | catalogue → Pydantic gate → M1 cleaning → Pandera zones → star schema → idempotent load **twice** (run-twice check) → aggregates → partitioned Parquet + CSV benchmark | exits 0 (~8 min) |
| `producer` | creates the 3 topics, replays `stream_events.json` at 400 msg/s, sends `EOS` | exits 0 (~30 s) |
| `consumer` | starts **only after the producer has exited** (the late-start test); validates, deduplicates, rejects, curates; stops at `EOS` | exits 0 |
| `windows` | tumbling one-minute windows per tenant from the curated topic | exits 0 |
| `spark` | the ten analyses, `explain()` and skew study, window check, validations, eleven metrics, detection scoring | exits 0 (~2 min) |

Every job writes its evidence to `output/` (mounted into the containers). Re-running any job is safe: loads are
upserts on natural keys, and Spark replaces its tables.

```bash
docker compose run --rm --no-deps warehouse                    # rebuild + reload (run-twice check included)
docker compose run --rm --no-deps consumer python -m kestrel_m2.stream.late_start_test   # full late-start test
docker compose run --rm --no-deps spark                        # analytics only
docker compose run --rm --no-deps warehouse python -m pytest -q   # the test suite inside the image
docker compose run --rm --no-deps warehouse python -m kestrel_m2.contracts_demo     # the failing-input demo
docker compose down -v                                         # stop and delete the database volume
```

### Look at the data

Adminer: <http://localhost:8080>, System **PostgreSQL**, Server `postgres`, user / password / database `kestrel`.
There are two schemas: **`kestrel_dw`** (warehouse, aggregates, stream tables) and **`kestrel_analytics`** (Spark
results).

```bash
docker compose exec postgres psql -U kestrel -d kestrel -c "select * from kestrel_dw.tenant_risk_metrics"
```

## 2. What you get

| Schema.table | Rows | Content |
|---|---:|---|
| `kestrel_dw.dim_tenant` / `dim_agent` / `dim_tool` / `dim_policy` / `dim_model_price` | 5 / 21 / 17 / 10 / 5 | dimensions; `dim_agent` one row per (agent_id, release_version), `dim_policy` one row per (profile, version) with validity dates as columns |
| `kestrel_dw.policy_allowed_tool`, `policy_egress_rule` | 33 / 19 | policy lists, UNNEST-ed in DuckDB |
| `kestrel_dw.dim_date`, `dim_time` | 61 / 1,440 | calendar and minute-of-day (6-hour bands) built from the data |
| `kestrel_dw.dim_source_file` | 11 | the source-file catalogue (file@sha256, size, rows, ingestion time) |
| `kestrel_dw.fact_session` | 100,000 | one row per accepted session (outcome, duration, counts, drift, first injection and violation times, cost) |
| `kestrel_dw.fact_prompt_turn` / `fact_tool_call` | 407,462 / 319,361 | turns (tokens, cost at turn-time price) / tool calls (policy version applied, violation) |
| `kestrel_dw.fact_network_flow` / `fact_sandbox_decision` / `fact_siem_alert` | 42,308 / 183,477 / 3,754 | flows / audit decisions / alerts with their correlation |
| `kestrel_dw.fact_rejected_record` | 13,589 batch + 747 stream | every refused record, one enum reason, file and line |
| `kestrel_dw.agent_daily_behaviour`, `tenant_risk_metrics`, `policy_violation_metrics`, `data_quality_metrics` | 805 / 5 / 23 / 57 | aggregates |
| `kestrel_dw.stream_curated_event`, `stream_message_log`, `stream_tenant_minute` | 8,827 / 9,577 / 30 | stream output |
| `kestrel_analytics.a01_...` … `a10_...`, `required_metrics`, `detection_evaluation` | | Spark results |

**Headline results**

* **Contracts.** 1,167,964 batch records validated at the readers, 5,171 refused at record level. **0
  disagreements** with the M1 reject ladder. 0 rows quarantined between zones in the clean run. The broken-input
  runs stop with exit 2 (structure) and exit 3 (stale data).
* **Warehouse.** The second load inserts, updates and deletes **0 rows**, and **21/21 tables** have identical row
  counts and MD5 checksums.
* **Storage.** CSV 239.4 MB → partitioned Parquet 33.0 MB in 60 daily files. On local disk DuckDB is 12–29× faster
  than on CSV, and a one-day query scans **1/60** files. Through a Docker Desktop bind mount, the 60 files pay per-file
  latency (measured and explained).
* **Stream.** 9,574 messages: **8,827 accepted, 747 rejected**, each with one reason (incl. 101
  `LATE_BEYOND_WATERMARK`, 98 `ORPHAN_EVENT`). The late-started consumer read from offset 0 thanks to
  `auto_offset_reset='earliest'`. Verdicts do not depend on partition interleaving.
* **Spark.** Broadcasting the dimensions takes a01 from 6.81 s to 0.98 s and removes the 34%-of-rows reducer. The
  window version of a04 equals the non-window version, and the validations pass (25/25, 4/4, 5/5).
* **Metrics.** M1's metrics 1, 2, 8 and 9 are reproduced exactly. The one difference, approximate p95 (−0.022 s), is
  explained. Detection R1 scores **precision 99.69% / recall 97.75%**. Estimated LLM cost is **USD 4,055.11**.

## 3. Run without Docker (development)

Python 3.13, from the repository root:

```bash
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r M2/requirements-dev.txt
cd M2
docker compose up -d postgres kafka                        # the services the jobs talk to
python -m kestrel_m2.build --check-idempotency             # warehouse (+ lake, benchmark)
python -m kestrel_m2.stream.late_start_test                # producer, late consumer, probes, windows
python -m kestrel_m2.contracts_demo                        # the failing-input demo
python -m pytest -q                                        # 46 tests (the DB test is skipped if Postgres is down)
docker compose run --rm --no-deps spark                    # Spark (on Windows, run it in Docker: Hadoop needs winutils)
```

| Variable | Default | Used by |
|---|---|---|
| `KESTREL_DB_URL` | `postgresql+psycopg2://kestrel:kestrel@localhost:5432/kestrel` | warehouse, consumer, windows, Spark (JDBC) |
| `KESTREL_KAFKA_BOOTSTRAP` | `localhost:9092` (`kafka:29092` in compose) | producer, consumer, windows |
| `KESTREL_RAW_DIR` | `../M1/data/raw` | every reader |
| `KESTREL_M2_OUT` | `output` | every job |
| `KESTREL_SPARK_JDBC_JAR` | `/opt/jars/postgresql-42.7.4.jar` in the image | Spark |

## 4. Repository layout

```
M2/
├── kestrel_m2/
│   ├── contracts.py        Pydantic models, error -> enum reason, stream field allowlist
│   ├── gate.py             the contract gate at the file readers (+ agreement with M1)
│   ├── schemas.py          Pandera curated + warehouse schemas, quarantine
│   ├── catalog.py          source-file catalogue
│   ├── policies_duckdb.py  sandbox_policies.json flattened in DuckDB
│   ├── star.py             warehouse spec (DDL, keys) and builder
│   ├── warehouse.py        idempotent PostgreSQL load, aggregates, checksums
│   ├── storage.py          partitioned Parquet, CSV benchmark, DuckDB queries
│   ├── build.py            the batch pipeline (entry point of the `warehouse` service)
│   ├── contracts_demo.py   deliberately broken inputs
│   ├── sql/                ddl.sql (generated), aggregates_ddl.sql, aggregates.sql
│   ├── stream/             producer, consumer, windows, late_start_test
│   └── spark/              common, analytics (ten analyses), metrics (eleven), detection
├── tests/                  pytest: contracts, zones, stream judge + windows, run-twice load
├── docs/                   data_dictionary.md, warehouse_columns.md, spark_plan.md
├── output/                 evidence (contracts, warehouse, storage, lake, stream, spark, metrics)
├── Dockerfile              python:3.13-slim + OpenJDK 21 + PostgreSQL JDBC (context = repo root)
└── docker-compose.yml
```

## 5. Libraries and images used

pandas, NumPy, PyArrow (frames, Parquet), Pydantic (record contracts), Pandera (zone schemas), DuckDB (policy
flattening, lake queries), SQLAlchemy and psycopg2 (PostgreSQL), kafka-python (producer, consumer, admin), PySpark
4.0 (analytics; PostgreSQL JDBC driver 42.7.4), pytest. Docker images: `postgres:16`, `adminer:4`,
`confluentinc/cp-zookeeper:7.6.0`, `confluentinc/cp-kafka:7.6.0`, `provectuslabs/kafka-ui`, `python:3.13-slim`.
Versions are pinned in `requirements*.txt`.
