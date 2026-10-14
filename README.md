# Kestrel · Milestone 1: Sources, Containers, Cleaning

GUC CSEN1095 Data Engineering, Winter 2026. This milestone builds pipeline stages 1 and 2. The nine batch files (four
formats, three clocks) are ingested and normalised, then cleaned and integrated into **one canonical activity table**
plus a **rejects table**. The pipeline runs reproducibly in Docker Compose (PostgreSQL + Adminer), and the four
Milestone 1 metrics are computed in pandas.

| Deliverable | Where |
|---|---|
| Profiling notebook (every file in its own format; problems stated before fixing) | [`notebooks/01_profiling.ipynb`](notebooks/01_profiling.ipynb) → `output/profiling/findings.csv` |
| Reusable cleaning module | [`kestrel_m1/`](kestrel_m1/) (one module per stage, tested in [`tests/`](tests/)) |
| `docker-compose.yml` + run instructions from a clean clone | [`docker-compose.yml`](docker-compose.yml), this README |
| Curated canonical activity table + rejects table | Postgres `kestrel.canonical_activity`, `kestrel.fact_rejected_record` (and `output/curated/*.parquet`) |
| Data dictionary (null, timezone, entity-resolution, dedup, correlation policies) | [`docs/data_dictionary.md`](docs/data_dictionary.md) |
| Data-quality findings (problem, detection, decision) | [`docs/dq_findings.md`](docs/dq_findings.md) |
| Metrics 1, 2, 8, 9, each with its policy | [`notebooks/02_metrics.ipynb`](notebooks/02_metrics.ipynb), [`output/metrics/`](output/metrics/) |

---

## 1. Run it from a clean clone

**Prerequisites:** Git and Docker (Docker Desktop on Windows/macOS) with Compose v2. Ports 5432 and 8080 must be free.

```bash
git clone <repository-url> kestrel
cd kestrel/M1
docker compose up --build
```

This starts three services:

| Service | What it does |
|---|---|
| `postgres` | PostgreSQL 16 on `localhost:5432` (user / password / database: `kestrel` / `kestrel` / `kestrel`) |
| `adminer` | web UI on <http://localhost:8080> |
| `pipeline` | one-shot job: reads `data/raw/`, runs every cleaning stage, writes `output/curated/*.parquet` and `output/metrics/*.csv`, loads 18 tables into schema **`kestrel`**, then **exits with code 0** (about 2 minutes) |

`postgres` and `adminer` keep running after the pipeline exits. Press `Ctrl+C` to stop, or use `docker compose up --build -d`
to run in the background and `docker compose logs -f pipeline` to follow the job. Re-running the pipeline replaces
every table, so it is idempotent:

```bash
docker compose run --rm pipeline
```

### Look at the data in Adminer

Open <http://localhost:8080> and log in with **exactly** these values:

| Field | Value |
|---|---|
| System | **PostgreSQL** (not the default MySQL) |
| Server | `postgres` |
| Username | `kestrel` |
| Password | `kestrel` |
| Database | **`kestrel`** |

The tables are in schema **`kestrel`**. Schema `public` and database `postgres` are empty, so if Adminer says
"No tables", check the *DB* and *Schema* selectors at the top. Direct link (asks for the password):
<http://localhost:8080/?pgsql=postgres&username=kestrel&db=kestrel&ns=kestrel>

Or from a terminal:

```bash
docker compose exec postgres psql -U kestrel -d kestrel \
  -c "select activity_type, count(*) from kestrel.canonical_activity group by 1 order by 2 desc"
```

### Stop / reset

```bash
docker compose down        # stop and remove the containers; the database volume is kept
docker compose down -v     # also delete the database volume (the next `up` rebuilds everything)
```

## 2. What you get

| Table (schema `kestrel`) | Rows | Content |
|---|---:|---|
| `canonical_activity` | 1,154,314 | every accepted record in the §5 row shape: 407,462 prompt turns, 319,361 tool calls, 197,952 lifecycle events, 183,477 sandbox decisions, 42,308 network flows, 3,754 SIEM alerts |
| `fact_rejected_record` | 13,589 | every refused record with **one** enum reason, source file, line number and raw payload |
| `stage_summary` | 15 | received = accepted + rejected, per source and reason |
| `stg_agent_activity`, `stg_network_flows`, `stg_sandbox_audit`, `stg_siem_alerts` | | per-source cleaned and enriched tables (all derived columns) |
| `session` | 100,000 | one row per accepted session (tenant, agent version, task, start) |
| `siem_correlation`, `siem_correlation_candidates`, `correlation_sensitivity` | | SIEM ↔ flow correlation per alert, every candidate pair, and counts for N = 5 s to 3,600 s |
| `dim_tenant`, `dim_agent` (SCD2, `agent_sk`), `dim_tool`, `dim_policy`, `dim_model_price` | | cleaned reference dimensions |
| `entity_resolution`, `audit_host_offsets` | | every merged/aliased key; measured audit-host clock offsets |

Column definitions and policies: [`docs/data_dictionary.md`](docs/data_dictionary.md).

**Headline results**

* Rejected: 12,977 of 937,752 activity rows (1.384%); 13,589 of 1,167,903 across the four event sources (1.164%).
* SIEM ↔ flow correlation at N = 60 s: **2,409 matched / 27 ambiguous / 1,318 unmatched**.
* Metrics (from [`output/metrics/m1_metrics.csv`](output/metrics/m1_metrics.csv)):

| # | Metric | Value |
|---|---|---|
| 1 | terminal sessions · success rate · error rate | 97,952 · 92.506% · 7.494% |
| 2 | session duration p50 · p95 | 17.646 s · 39.560 s |
| 8 | trace completeness · correlation coverage | 95.881% · 64.864% (2,435 / 3,754 alerts) |
| 9 | rejected rate (activity) · late-event rate | 1.384% · 1.499% |

The policy behind each value (denominator, population, late-event and null policy) is in
[`output/metrics/m1_metric_policies.csv`](output/metrics/m1_metric_policies.csv) and explained in
[`notebooks/02_metrics.ipynb`](notebooks/02_metrics.ipynb).

## 3. Run without Docker (development)

Python 3.14:

```bash
cd M1
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt                     # requirements.txt = pipeline only

python -m kestrel_m1.pipeline --no-db    # all stages -> output/curated/*.parquet + output/metrics/*.csv (~40 s)
python -m kestrel_m1.pipeline            # same, plus the Postgres load (needs `docker compose up -d postgres`)
python -m kestrel_m1.metrics             # recompute the metrics only, from output/curated/
python -m pytest -q                      # 47 tests, ~30 s (includes full-pack checkpoints)
jupyter lab notebooks/                   # 01_profiling, 02_metrics (02 builds output/curated if missing)
```

Configuration (environment variables, all optional):

| Variable | Default | Used by |
|---|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | `kestrel` | compose (database and pipeline connection) |
| `KESTREL_DB_URL` | `postgresql+psycopg2://kestrel:kestrel@localhost:5432/kestrel` | pipeline |
| `KESTREL_DB_SCHEMA` | `kestrel` | pipeline |
| `KESTREL_OUT_DIR` | `output/curated` | pipeline |

## 4. How the pipeline works

```
data/raw/*  ──readers.py──►  raw frames (+ source_file, source_line)
                │
reference.py ───┤  tenants, agents (SCD2 agent_sk), tools, pricing, policies (validity windows)
                ▼
activity.py ──► accepted activity + rejects ──► session_table (accepted session.start rows)
                                                   │
flows.py  ◄────────────────────────────────────────┤  orphan check, inherit tenant / agent_sk
audit.py  ◄──────────── + accepted tool calls ─────┘  per-host clock offset measured from the data
siem.py   ──► alerts in UTC ──► correlate.py (alert × flow, N = 60 s)
                │
policy.py ──► policy version at event time, is_policy_violation, risk_indicators, cost_usd
canonical.py ─► canonical_activity (§5)          rejects.py ─► fact_rejected_record (closed enum)
pipeline.py ──► Parquet + PostgreSQL (COPY)      metrics.py ─► output/metrics/
```

* **Nothing is dropped silently.** Every received record is either accepted or rejected, and the pipeline asserts
  `accepted + rejected = received` per source. Non-fatal repairs are kept in `quality_flags`.
* **One reason per reject**, the first that applies in the closed enum order (`rejects.REASONS`).
* **All times in UTC.** The data dictionary records how each source's clock was established.
* `stream_events.json` and `incident_labels.csv` are in `data/raw/` but **not used in M1**. The labels are held-out
  ground truth and never feed the pipeline.

## 5. Repository layout

```
M1/
├── data/raw/              the nine batch files + two M2 files (committed; `-text` keeps line numbers exact)
├── kestrel_m1/            the cleaning package (readers, timeutil, reference, rejects, activity, flows, audit,
│                          siem, correlate, policy, canonical, metrics, pipeline, profiling)
├── notebooks/             01_profiling.ipynb, 02_metrics.ipynb
├── tests/                 pytest suite (unit tests + full-pack checkpoints)
├── docs/                  data_dictionary.md, dq_findings.md
├── output/
│   ├── profiling/         findings log from the profiling notebook
│   ├── metrics/           metric values, policies, missing-token effect (CSV)
│   └── curated/           Parquet tables (generated, git-ignored)
├── Dockerfile             image for the `pipeline` service (python:3.14-slim)
├── docker-compose.yml     postgres + adminer + pipeline
├── requirements.txt       pipeline dependencies (pinned)
└── requirements-dev.txt   + jupyter, pytest
```

## 6. Libraries and images used

pandas, NumPy and PyArrow (data processing, Parquet), SQLAlchemy and psycopg2 (PostgreSQL load), Jupyter and nbclient
(notebooks), pytest (tests). Docker images: `postgres:16`, `adminer:4`, `python:3.14-slim`. Versions are pinned in
`requirements*.txt`.
