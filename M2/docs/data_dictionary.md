# Kestrel M2 · Data dictionary

This document records every choice made in Milestone 2 and the reason for it. It builds on the
[M1 data dictionary](../../M1/docs/data_dictionary.md), whose policies still apply unless a section here says
otherwise: timezone, deduplication, entity resolution, nulls, the reject order, late events, correlation and the
missing-token policy. Column-level listings of every warehouse table are generated from the code in
[`warehouse_columns.md`](warehouse_columns.md). The Spark plan is explained in [`spark_plan.md`](spark_plan.md).

Numbers quoted here come from the committed outputs in [`../output/`](../output/).

---

## 1. Zones and where each contract sits

```
raw files ──Pydantic gate (every record)──► M1 cleaning ──Pandera "curated"──► star model ──Pandera "warehouse"──► PostgreSQL kestrel_dw
    │            structural break: stop (exit 2)      table-level break: stop (exit 3)       table-level break: stop (exit 3)
    └─ catalogue (file@sha256)          rejects + quarantined rows ─────────────────────────────► fact_rejected_record

stream_events.json ─producer─► Kafka raw ─consumer (Pydantic per message + state)─► curated topic / stream_curated_event
                                                                   └──────────────► rejected topic / fact_rejected_record
```

| Zone | Holds | Contract at its entry |
|---|---|---|
| raw | the pack, byte for byte (`M1/data/raw`) | catalogue + Pydantic model per record type (`contracts.py`, `gate.py`) |
| curated | M1's stage tables (`stg_*`, `session`, dims) | Pandera curated schemas (`schemas.curated_schemas`) |
| warehouse | star schema `kestrel_dw` | Pandera warehouse schemas, generated from the table spec (`schemas.warehouse_schema`) |
| stream | `kestrel.agent.events*` topics, `stream_*` tables | Pydantic `ActivityEvent` / `EndOfStream` per message + consumer state |

---

## 2. Contracts (Lab 4)

### 2.1 Pydantic: one model per record type

| Source | Model | Notable rules |
|---|---|---|
| `tenants.csv` | `Tenant` | `tn_*`; enums; invalid region **repaired** to null (`INVALID_ENUM_REGION`, M1 T2) |
| `agents.csv` | `Agent` | `ag_*`; semver after stripping `v` (**repair** `VERSION_PREFIX_STRIPPED`); enums; budget ≥ 0 |
| `tools.csv` | `Tool` | `tl_*`; semver; category / risk-tier enums; `true`/`false` booleans |
| `model_pricing.csv` | `ModelPrice` | prices nullable (M1 P1) but ≥ 0; currency `USD` |
| `sandbox_policies.json` | `SandboxPolicy` (+ `PolicyNetwork`, `PolicyFilesystem`, `PolicyResources`) | nested; `network` optional (no block = no egress); ports 1..65535; `extra="forbid"` |
| `agent_activity.parquet` | `ActivityEvent` = discriminated union on `event_type` of `SessionStart`, `LlmTurn`, `ToolCall`, `SessionEnd`, `SessionError` | each variant declares its own required fields (Dataset Description 3.5 "filled on"); ranges on every count; per-type status enum; `succeeded` **repaired** |
| `network_flows.csv` | `NetworkFlow` | ids, IPv4, port 1..65535, byte counts ≥ 0, enums |
| `sandbox_audit.log` | `AuditDecision` | `-` is null; `Mon DD HH:MM:SS` strictly parsed; decision/action enums |
| `siem_alerts.json` | `SiemExport` (envelope) + `SiemAlert` (+ `SiemDestination`, `SiemTechnique`) | `detected_at` needs an explicit offset; confidence 0..1 |
| `incident_labels.csv` | `IncidentLabel` | label / stage enums (read only by the evaluation step) |
| `stream_events.json` | `ActivityEvent` + `EndOfStream` | as the batch rows, and epoch-ms may arrive as a JSON integer |

**Error → reason mapping** (`contracts.reason_of_error`). A key that is missing, or a required field that is null,
gives `MISSING_REQUIRED_FIELD`. The custom `malformed_timestamp` error gives `MALFORMED_TIMESTAMP`. An unknown
`event_type` tag gives `INVALID_EVENT_TYPE`; a null tag is a missing field. A `ge`/`le` failure gives
`INVALID_NUMERIC_RANGE`. Anything else (enum, pattern, wrong type) gives `SCHEMA_VIOLATION`. When a record has several
errors, the **first reason in the closed enum order** wins (`rejects.REASONS`, the same order as M1). For a record
with an invalid `event_type`, the union stops at the tag, so the record is validated a second time against a
permissive model to collect its other errors. That way a malformed timestamp still outranks the invalid type.

**Repairs are never silent.** A validator that normalises a value records it in the validation context
(`check(...).repairs`), and those names become quality flags downstream.

**Cross-record reasons** (`DUPLICATE_EVENT_ID`, `UNKNOWN_*_ID`, `ORPHAN_EVENT`, `LATE_BEYOND_WATERMARK`) need state,
so they are decided by the stage that holds that state: the M1 ladder in batch, the consumer in the stream. They are
never decided by a single-record model.

### 2.2 The gate at the file readers (`gate.py`)

1. **Structure first.** The Parquet physical schema must equal the contract (`readers.EXPECTED_ACTIVITY_SCHEMA`),
   CSV headers must equal the documented columns, and the SIEM envelope must validate. A structural break cannot be
   quarantined record by record, so the build stops with **exit code 2**.
2. **Every record** of the nine batch files goes through its model: 1,167,964 records, 5,171 refused at record level
   ([`output/contracts/reader_gate_summary.csv`](../output/contracts/reader_gate_summary.csv)).
3. **Agreement with M1.** A record-level reason found by the contract must equal M1's reason, unless M1 gave an
   earlier cross-record reason. Result: **0 disagreements on all 1,167,964 records**
   ([`reader_gate_disagreements.csv`](../output/contracts/reader_gate_disagreements.csv) is empty). The contract and
   M1's vectorised ladder are two independent implementations of the same Dataset Description, and they agree.

The refused records are the raw zone's quarantine: they are in `fact_rejected_record` with `zone = 'raw'`,
`detected_by = 'pydantic_gate+m1_ladder'`, file, line and raw payload.

### 2.3 Pandera between zones (`schemas.py`)

Each check is named `<ENUM_REASON>:<what>`, so a failure maps to exactly one enum reason. Pandera's built-in
`not_nullable` maps to `MISSING_REQUIRED_FIELD`.

| Check kind | Curated zone (M1 `stg_*`) | Warehouse zone (every star table, generated from `star.TABLES`) | Reason |
|---|---|---|---|
| types | dtype of timestamps (`datetime64[ms, UTC]`), counts (`Int64`), floats | dtype of **every** column; `strict=True, ordered=True` (no extra or missing column) | table-level → stop |
| nullability | required columns not null | the spec's `NOT NULL` | `MISSING_REQUIRED_FIELD` |
| uniqueness | `event_id`, `flow_id`, `audit_id`, `alert_id` | the primary key | `DUPLICATE_EVENT_ID` (later copies only; the first is kept, as in M1) |
| ranges | `sequence_number ≥ 1`, counts / tokens / cost / latency ≥ 0, port 1..65535, confidence 0..1 | same, plus durations, prices, sizes | `INVALID_NUMERIC_RANGE` |
| enums | event type, status per event type, verdict, protocol, decision, action, severity | outcome, verdict, decision, severity, match status, zone, channel, content source, finish reason | `INVALID_EVENT_TYPE` / `SCHEMA_VIOLATION` |
| referential | tenant ∈ `dim_tenant`, `(agent_id, release_version)` ∈ `dim_agent`, session ∈ accepted sessions | every foreign key of the spec, checked against the **already validated** parent (dependency order) | `UNKNOWN_TENANT_ID`, `UNKNOWN_AGENT_ID`, `ORPHAN_EVENT`, `MALFORMED_TIMESTAMP` (date/time keys), else `SCHEMA_VIOLATION` |
| time window | event time in [2026-01-31, 2026-04-01) | same, on each fact's event time | `MALFORMED_TIMESTAMP` |
| freshness | newest in-window record within **1 day** of the expected end (2026-04-01 00:00 UTC) | same, per fact | table-level → stop |

**Enforcement** (`schemas.enforce`). Validation is lazy, so all failures are collected. A failure that belongs to a
row quarantines that row, with one reason, the failed check and value in `reason_detail`, and the row as JSON in
`raw_payload`, into `fact_rejected_record` (`zone` = curated / warehouse, `detected_by = 'pandera'`). The row is
removed from that zone's output, and its children then fail their foreign-key check, so nothing dangles. A failure
of the table as a whole cannot be pinned on a row, so it raises `ZoneContractError` and the build stops with
**exit code 3**.

**Freshness reference.** The pack is a closed extract, so "fresh" is measured against its documented end
(`config.PACK_END`), not against the wall clock. In production the reference would be the load time. A truncated
or stale extract fails this check.

In the clean run, **0 rows** are quarantined in either zone: M1's cleaning already meets the contract.

### 2.4 Failing on a deliberately broken input

[`output/contracts/broken_input_demo.md`](../output/contracts/broken_input_demo.md) (`python -m kestrel_m2.contracts_demo`):

* **Record level.** A real tool-call row is broken in 11 ways and each copy gets one reason. For example,
  `retry_number = -1` together with a null `status` gives `MISSING_REQUIRED_FIELD`, because it is earlier in the
  enum.
* **Row level.** In 1,000 real `fact_tool_call` rows, 7 are broken: a duplicate key, negative latency, an unknown
  tenant, an orphan session, a 2027 timestamp, a null status and `retry_number = -1`. Exactly those 7 are
  quarantined, each with one reason.
* **The pipeline failing.** The real build was run on a copy of the pack. When `agent_activity.parquet` loses
  `ingest_time` and stores `sequence_number` as text, the build stops with **exit 2** at the reader. When
  `network_flows.csv` is truncated to rows before 15 February, the build gets through M1 cleaning and then stops with
  **exit 3** on the curated freshness check.

---

## 3. Source-file catalogue (Lab 6)

Table **`dim_source_file`** (also [`output/warehouse/source_file_catalog.csv`](../output/warehouse/source_file_catalog.csv)):
one row per **file content**.

| Column | Meaning |
|---|---|
| `source_file_id` (PK) | `<file name>@<first 12 hex of SHA-256>`: names the exact bytes |
| `file_name`, `path` | name, and path relative to the repository |
| `format`, `format_detail`, `grain` | `csv` / `parquet` / `json` / `syslog`; reader kind; what one record is |
| `size_bytes`, `content_sha256` | size and full SHA-256 hash of the content |
| `row_count` | records as the reader sees them (CSV data rows, Parquet rows, array elements, alerts, log lines) |
| `ingested_at` | **first** time this content was ingested |

**How the warehouse points back.** Every fact row and every reject carries `source_file_id` (FK to the catalogue)
and `source_line`. Every dimension row carries them too. So any row can be traced to one line of one exact file
version. If a file changes, it gets a new id and a new catalogue row, and the old row stays. The catalogue is
written **insert-once** (`ON CONFLICT DO NOTHING`), which keeps the original `ingested_at` and keeps the load
idempotent.

---

## 4. Warehouse (Lab 5): schema `kestrel_dw`

### 4.1 Model

```
                 dim_date ─┐   dim_time ─┐      dim_source_file (catalogue) ◄── every table: source_file_id, source_line
                           │             │
dim_tenant ◄──┬── fact_session ◄──────────┼──── fact_prompt_turn
              │     │  (one row per       ├──── fact_tool_call ──► dim_tool (tool_id, tool_version)
dim_agent ◄───┤     │   accepted session) ├──── fact_network_flow ◄── fact_siem_alert (matched_flow_id)
(agent_id,    │     │                     └──── fact_sandbox_decision
 release_     │     └─► dim_policy (sandbox_profile_id, policy_version) ◄── policy_allowed_tool, policy_egress_rule
 version)     │
              └── fact_rejected_record (batch + stream, one enum reason)      dim_model_price (model_id, effective_from)
```

| Table | Grain | Natural key = PK (what the upsert conflicts on) | Rows |
|---|---|---|---:|
| `dim_tenant` | canonical tenant | `tenant_id` | 5 |
| `dim_agent` | **agent version** | `(agent_id, release_version)`; `agent_sk` kept as a unique surrogate | 21 |
| `dim_tool` | tool version | `(tool_id, tool_version)` | 17 |
| `dim_policy` | **policy version** | `(sandbox_profile_id, policy_version)` | 10 |
| `policy_allowed_tool`, `policy_egress_rule` | one list element of a policy version | (profile, version, tool / entry) | 33 / 19 |
| `dim_model_price` | price point | `(model_id, effective_from_utc)` | 5 |
| `dim_date` | UTC day, built from the data's own timestamps | `date_key` (yyyymmdd) | 61 |
| `dim_time` | minute of the UTC day, with 6-hour bands | `time_key` (0..1439) | 1,440 |
| `dim_source_file` | file content | `source_file_id` | 11 |
| `fact_session` | accepted session | `session_id` | 100,000 |
| `fact_prompt_turn` | accepted `llm.turn` | `event_id` | 407,462 |
| `fact_tool_call` | accepted `tool.call` (each retry is a row) | `event_id` | 319,361 |
| `fact_network_flow` | accepted egress attempt | `flow_id` | 42,308 |
| `fact_sandbox_decision` | accepted audit line | `audit_id` (`L<line>`) | 183,477 |
| `fact_siem_alert` | accepted alert | `alert_id` | 3,754 |
| `fact_rejected_record` | refused record (batch and stream) | `(source_file, source_line)` | 13,589 batch + 747 stream |

**Key choices.**

* **Natural keys as primary keys.** The upsert needs a declared natural key, and the source ids (`event_id`,
  `flow_id`, `alert_id`) are stable across re-deliveries, so they survive into the warehouse unchanged. The syslog has
  no id, so `audit_id = L<line>` (M1 policy). It is stable because the raw file is committed byte-for-byte.
* **`dim_agent` keyed on `(agent_id, release_version)`**, as the brief asks. Facts carry `agent_id` and
  `release_version` (a composite FK) plus `agent_sk`. The version is the one that actually ran (one container per
  session), not the one "current" at load time. The Type-2 validity window `[valid_from_utc, valid_to_utc)` is kept
  as ordinary columns. Rows outside it are flagged `AGENT_VERSION_OUTSIDE_WINDOW`, never re-keyed.
* **`dim_policy` keyed on `(sandbox_profile_id, policy_version)`.** Facts carry `sandbox_profile_id` and
  `policy_version_applied`, the version whose effective window contains the event time (M1 §2.4). The composite FK
  makes **"was this call allowed then?"** a plain join:

  ```sql
  SELECT c.event_id, c.tool_id, c.policy_version_applied, (a.tool_id IS NOT NULL) AS allowed_then
  FROM kestrel_dw.fact_tool_call c
  LEFT JOIN kestrel_dw.policy_allowed_tool a
         ON a.sandbox_profile_id = c.sandbox_profile_id AND a.policy_version = c.policy_version_applied
        AND a.tool_id = c.tool_id;
  ```

  On every tool call, this join agrees with the stored `is_policy_violation`. Both the declared validity dates and
  the effective (overlap-resolved) ones are ordinary columns.
* **No FK from `dim_agent` to `dim_policy`.** `ag_019` points to `sp_missing_profile`, which does not exist (M1 AG2).
  The agent is kept and its records have no policy in effect. A composite fact FK with a null
  `policy_version_applied` is not checked (SQL `MATCH SIMPLE`), which is exactly that policy.
* **Date and time keys.** Facts carry `date_key` and `time_key` for their own event time (UTC). Sessions use their
  start time. The calendar is built from the data (2026-01-31 to 2026-04-01, the stream's day included).
* **Categorical values stay as source values.** Encodings are for preprocessing only, and nothing is label-encoded
  in the warehouse.

**Denormalisation, and why.**

* **`fact_session` is an accumulating snapshot.** It holds counts of turns, tool calls, violations, flows, distinct
  destinations, decisions, denials, escalation attempts, injection turns and late events, plus `outcome`,
  `duration_s`, trace completeness, goal drift, the first injection and first violation times, and tokens and cost.
  Six of the eleven metrics and most analyses are per session. Computing them once at load time, with M1's own
  functions (`kestrel_m1.metrics.session_outcomes`), keeps every consumer on the same definition, and the numbers
  match M1 exactly.
* **Tenant, agent version and policy version are copied onto flow and audit facts**, which have no agent of their
  own in the source. They are inherited from the session, as in M1, so these facts join to the dimensions without
  going through `fact_session`.
* **`fact_siem_alert` carries the correlation result** (status, candidate counts, matched flow, session, Δt).
  Correlation is a property of the alert, and keeping it on the alert keeps alerts without a session queryable.
* **Policy lists** are stored as PostgreSQL arrays in `dim_policy` (readable) and also as bridge tables (joinable).

### 4.2 Idempotent load (`warehouse.load`)

All tables are loaded in **one transaction**:

1. COPY each table into a temporary staging table.
2. **Upsert on the natural key:** `INSERT ... ON CONFLICT (pk) DO UPDATE SET ... WHERE (t.*) IS DISTINCT FROM
   (EXCLUDED.*)`. An identical row is **not touched**, so even `loaded_at` does not change.
3. **Prune** keys that are no longer produced, children first. `fact_rejected_record` is pruned only in its batch
   partition (`channel = 'batch'`), so stream rejects survive a batch reload.
4. **Aggregates** are rebuilt with delete-then-insert of the whole table (one partition) by SQL
   (`sql/aggregates.sql`). Rates are stored as `numeric(9,6)`, so a rebuild is reproducible to the bit.

**Run-twice test.** The pipeline itself runs `--check-idempotency`. On the full data it loads a second time and
compares, per table, the row count and an MD5 over all rows in key order, `loaded_at` included
([`output/warehouse/idempotency_check.csv`](../output/warehouse/idempotency_check.csv)). Result: **21/21 tables
identical, and the second load inserted, updated and deleted 0 rows.** The pytest version
(`tests/test_idempotent_load.py`) does the same on a 1,500-session sample in a scratch schema. It also checks that a
changed row is updated (1) and a vanished row pruned (1), with nothing else touched.

### 4.3 Aggregates

| Table | Grain | Content |
|---|---|---|
| `agent_daily_behaviour` | (UTC day, agent version) | sessions started / terminal / success / error, turns, injection turns, tool calls, violations, denials, failures, retries, flows, egress violations, sandbox decisions, denials, escalation attempts, tokens, cost, and four rates |
| `tenant_risk_metrics` | tenant | session error rate, tool-violation, egress-violation and denial rates, escalation-denied sessions, injection-turn rate, goal-drift rate, injection→violation sessions, linked SIEM alerts (all, high/critical), tokens, cost |
| `policy_violation_metrics` | (policy version in effect, activity type) | records, violations, rate, sessions with a violation, `rule_breakdown` (e.g. `no_egress_allowed=3718`) |
| `data_quality_metrics` | (channel, source file, metric, code) | received, accepted, rejected per reason, late, quality flags; batch and stream |

---

## 5. Policies flattened in DuckDB (Lab 6)

`policies_duckdb.py` reads `sandbox_policies.json` with DuckDB's `read_json`. The nested objects become STRUCTs and
the allowlists LISTs, so flattening is SQL. Struct fields are selected with dot notation. A missing `network` gives
`has_network_block = false` and empty lists, because "no network block = no egress" must survive the flatten. The
overlap rule (newer version wins, M1 PO1b) is a `LEAD()` window. `UNNEST` produces the two bridge tables. Each
wildcard entry also gets `match_regex` (`*` = exactly one DNS label, anchored, M1's rule), so SQL can test hosts
with `regexp_matches`. The DuckDB result is compared field by field with M1's pandas flatten. **0 differences**
([`output/warehouse/policy_flatten_duckdb_vs_m1.csv`](../output/warehouse/policy_flatten_duckdb_vs_m1.csv) is empty).
It is loaded as `dim_policy`.

---

## 6. Analytical storage (Lab 5): partitioned Parquet

`storage.py` writes the curated `canonical_activity` table (1,154,314 rows) as **hive-partitioned Parquet**, at
`output/lake/canonical_activity/event_date=YYYY-MM-DD/part-0.parquet`. Compression is zstd. Rows are sorted by
`(event_time_utc, activity_id)`, so row-group statistics also prune time ranges inside a day. File names are
deterministic and partitions are replaced on rewrite, so the write is idempotent.

| Layout | Files | Size |
|---|---:|---:|
| CSV (one file) | 1 | 239.4 MB |
| Parquet, one file (zstd) | 1 | 23.6 MB |
| **Parquet partitioned by `event_date`** | **60** | **33.0 MB** |

DuckDB, median of 5 warm runs inside the pipeline container ([`output/storage/benchmark.csv`](../output/storage/benchmark.csv);
queries and answers in [`duckdb_queries.md`](../output/storage/duckdb_queries.md), where each answer is asserted
identical on all three layouts). The layouts are measured twice. The first measurement uses a copy on the
container's **local disk**, which measures the layouts themselves. The second reads the `output/` directory as written,
which in docker compose is a **bind mount** from the host.

| Query | CSV | Parquet (1 file) | Parquet partitioned | Partitioned, via the Docker Desktop bind mount (1 file / 60 files) |
|---|---:|---:|---:|---:|
| Q1 one day (partition pruning) | 277 ms | 15 ms | **9 ms** | 133 / 685 ms |
| Q2 full-scan aggregate | 276 ms | 49 ms | **24 ms** | 172 / 736 ms |
| Q3 two-column projection | 264 ms | 24 ms | **18 ms** | 159 / 806 ms |
| Q4 one-week range | 264 ms | 21 ms | **11 ms** | 64 / 570 ms |
| Q5 one-session lookup | 288 ms | 35 ms | **17 ms** | 222 / 676 ms |

`EXPLAIN ANALYZE` of Q1 on the lake shows `File Filters: (event_date = '2026-03-14')` and **`Scanning Files: 1/60`**.

**What the partition key did to the file count** ([`partition_key_file_counts.csv`](../output/storage/partition_key_file_counts.csv)):

| Partition key | Files | Median file | Largest / median |
|---|---:|---:|---:|
| none | 1 | 23.6 MB | 1.0 |
| `event_month` | 3 | 14.6 MB | 1.1 |
| **`event_date`** | **60** | **0.55 MB** | **1.1** |
| `activity_type` | 6 | 4.3 MB | 3.2 |
| `tenant_id` | 6 | 3.7 MB | **5.2** (tn_001 holds most rows) |
| `event_date` + `activity_type` | 360 | 0.08 MB | 3.2 |
| `event_date` + `tenant_id` | 325 | 0.08 MB | 4.7 |

**Choice: `event_date`.** Almost every question is time-bounded (daily metrics, weekly trends, one incident's day),
and the traffic is spread evenly over the days. That gives 60 files of almost equal size, with no skew between
files. Partitioning by tenant or activity type inherits the data's skew: one tenant file is 5× the median. Adding a
second key multiplies the files to 325–360 tiny ones (about 80 KB each), which costs more in per-file overhead and
row-group headers than pruning saves. Partitioning adds about 9 MB over one file, because each file carries its own
dictionaries and footer. On a local disk the lake wins every query: it prunes whole files, and DuckDB reads the
files in parallel. The sorted single file comes second, because its row-group statistics already prune time ranges.
**File count has a price where opening a file is expensive.** Through Docker Desktop's bind mount, where every open
crosses the VM boundary, the 60-file lake is 4–9× slower than the single file. On object storage the same trade-off
argues against the 325–360-file layouts even more strongly. One file per day is the compromise.

---

## 7. Streaming (Lab 7)

### 7.1 Topology (Appendix B)

| Topic | Partitions | Key | Written by | Content |
|---|---:|---|---|---|
| `kestrel.agent.events` | 3 | `session_id` | producer | the replay (sanitised), then `EOS` on every partition |
| `kestrel.agent.events.curated` | 3 | `session_id` | consumer | accepted, normalised records, then `EOS` per partition |
| `kestrel.agent.events.rejected` | 3 | `session_id` | consumer | refused records, with one reason, file and line |

Consumer group **`kestrel-tata`** (one per team). Topics are created explicitly (`auto.create.topics.enable=false`).

### 7.2 Producer policy

* **Fixed pace.** One array element = one message, in file order (the file is ordered by `ingest_time`), at a fixed
  400 msg/s (`--rate`): 9,574 messages in 24 s. Headers: `source_file`, `source_line` (the physical line where the
  element starts), `replay_seq`, `producer_id`.
* **`EOS`** is sent last, **to every partition**, so each partition's reader knows the replay is complete.
* **No raw prompt or response text on the stream.** The producer forwards an explicit allowlist of operational
  fields (`contracts.STREAM_FIELDS`). `stated_goal` is the only free text in the contract: the goal the model wrote,
  so model output. It is replaced by `stated_goal_fingerprint`, the first 16 hex characters of its SHA-256, which
  shows *that* a goal changed but not what it says (4,215 records). Unknown fields are dropped. The 68 elements that
  are truncated JSON strings are forwarded as the strings they are, after removing any `"stated_goal": "..."`
  fragment (none was present), so the consumer can reject them. As a second line of defence, the consumer never
  copies text into a curated record, and flags `RAW_TEXT_DROPPED` if a message still carries `stated_goal`.

### 7.3 Consumer: one reason per refused record

Order of judgement per message (`stream/consumer.py`, `StreamJudge.process`):

1. Not JSON, or not a JSON object → `UNPARSEABLE_RECORD` (68: truncated records). A valid `EOS` marks its partition
   as done.
2. The Pydantic `ActivityEvent` contract (§2.1) gives the record-level reasons. On the stream, the required
   `stated_goal` is satisfied by a non-null fingerprint.
3. State: `DUPLICATE_EVENT_ID` if the `event_id` was already seen **at another (partition, offset)**. The first copy
   is the earliest `ingest_time`, because the feed is ordered by it. A redelivered message (same partition and
   offset) is skipped, not called a duplicate. `UNKNOWN_AGENT_ID` and `UNKNOWN_TENANT_ID` are checked against
   `dim_agent` and `dim_tenant` read from the warehouse, after alias and `v` normalisation.
   `LATE_BEYOND_WATERMARK` is described in §7.4.
4. **Orphans need buffering.** In 38 sessions the `session.start` arrives *after* some of the session's events. A
   non-start event whose session has no accepted start *yet* is held back, not rejected. It is released (and judged)
   when the start is accepted (35 released). It becomes `ORPHAN_EVENT` when its partition's watermark passes its
   `event_time`, because its start cannot be later than it and could then only be rejected as late. Anything still
   held at the end of the stream is also an orphan (98 in total).
5. Exactly one reason per record: the first applicable reason in the closed enum order.

Accepted records are normalised and written to the curated topic and to **`stream_curated_event`**. Normalisation
covers UTC ISO timestamps (epoch-ms integers and `dd/mm/yyyy` converted, `NONSTANDARD_TIMESTAMP_FORMAT`), tenant
alias, the `v` prefix, the tool alias (`http_get` → `tl_web_fetch`), `succeeded` → `success`, and the task copied
from the start, plus the M1 quality flags. Refused records go to the rejected topic and to `fact_rejected_record`
with `channel = 'stream'`, `zone = 'stream'`, `source_file = 'stream_events.json'` and the element's `source_line`,
plus the Kafka topic, partition and offset.

### 7.4 Watermark: 60 s on event time, per partition

The watermark is kept per partition, like Kafka Streams' *stream time*. For each partition it is the max `event_time`
accepted from that partition, minus 60 s. A record whose `event_time` is older is rejected as
`LATE_BEYOND_WATERMARK`. Lateness is always measured on `event_time`, never on `ingest_time`.

*Why per partition.* With a single global max, our first real run rejected 127 records as late and 164 as orphans,
against 101 and 98 when the same feed was replayed in file order. A consumer that starts late reads each partition in
bursts, so one partition that had been read further made the others' on-time records look late. That is an artefact
of consumption order, not of the data. Every session lives in one partition (key = `session_id`) and each partition
is in ingest order, so per-partition stream time makes **every verdict independent of how the partitions are
interleaved**. We tested this: identical verdicts per message for file order, partition-after-partition and random
chunks (`tests/test_stream.py`).

**A record caught by the watermark** ([`output/stream/watermark_caught_example.json`](../output/stream/watermark_caught_example.json)):
`ev_0933714`, an `llm.turn` of `tn_002` with `event_time` 00:10:08.000, ingested at 00:27:56.589 on partition 0. The
partition's stream time was 00:27:55.176, so the watermark was 00:26:55.176. The record was **1,007 s behind** it and
was rejected `LATE_BEYOND_WATERMARK`. Records more than 10 s late but within the watermark are accepted and flagged
`LATE` (68, 0.77% of accepted).

### 7.5 At-least-once and offsets

`enable_auto_commit = False`. After each poll batch, the consumer:

1. flushes the curated and rejected topics and checks every send future;
2. commits the PostgreSQL transaction (upserts into `stream_curated_event`, `fact_rejected_record` and
   `stream_message_log`);
3. **only then** commits the offsets.

The committed offset of a partition never passes a message that is still held back for its session start. After a
crash, redelivered messages are either already in `stream_message_log` (skipped) or reprocessed into upserts on
natural keys. Either way a replay does no harm, and the state is rebuilt from the database
(`consumer.restore_state`).

### 7.6 The late-start test

[`output/stream/late_start_test.md`](../output/stream/late_start_test.md) (`python -m kestrel_m2.stream.late_start_test`,
and the order of the compose services). The producer finished before the consumer started.

| Run | Group | `auto_offset_reset` | First offset read | Messages read | Accepted / rejected |
|---|---|---|---|---:|---|
| 1 | `kestrel-tata` (new) | `earliest` | 0, 0, 0 | 9,577 | 8,827 / 747 (+ 3 EOS) |
| 2 | new probe group | `latest` | none | 0 | — |
| 3 | `kestrel-tata` again | `earliest` | none | 0 | — |

**What `auto_offset_reset` did.** It only applies when a group has no committed offset. In run 1 the new group had
none, so `earliest` started it at offset 0 of every partition, and it consumed the whole replay although it was not
running when the data was produced: Kafka retains the log. In run 2, `latest` started a fresh group at the end of the
log, so it would have lost the entire feed. In run 3 the group's committed offsets (3000 / 3521 / 3056) take
precedence and `auto_offset_reset` is not consulted, so nothing is processed twice.

Reasons in run 1: DUPLICATE_EVENT_ID 66 · UNKNOWN_AGENT_ID 68 · UNKNOWN_TENANT_ID 68 · MALFORMED_TIMESTAMP 33 ·
ORPHAN_EVENT 98 · INVALID_EVENT_TYPE 68 · MISSING_REQUIRED_FIELD 66 · INVALID_NUMERIC_RANGE 68 ·
LATE_BEYOND_WATERMARK 101 · UNPARSEABLE_RECORD 68 · SCHEMA_VIOLATION 43. Total 747 of 9,574 (7.80%).

### 7.7 Tumbling windows (bonus)

`stream/windows.py` reads the **curated** topic and counts events, tool calls and denied tool calls per tenant per
one-minute event-time window `[start, start + 60 s)`, into **`stream_tenant_minute`**
([`output/stream/tenant_minute_windows.csv`](../output/stream/tenant_minute_windows.csv): 30 windows, 30 denied tool
calls).

**How the window handles late data.**

* Nothing more than 60 s behind the consumer's watermark reaches the curated topic. Such records were rejected as
  `LATE_BEYOND_WATERMARK` and are visible in `fact_rejected_record`.
* The window job keeps its own watermark: the minimum over partitions of each partition's max event time, minus 60 s,
  so a lagging partition holds windows open. A window stays **open** until that watermark passes its end, so for 60 s
  of event time after the window closes. Records arriving in that time are added to it. Then the window is emitted
  once with `is_final = true`.
* A record that still arrives for a final window is not dropped: the window is corrected (upsert) and
  `late_updates` goes up, so the correction is visible. In this run, `late_updates = 0`. At `EOS` every open window
  is finalised.

### 7.8 Stream tables

| Table | Key | Content |
|---|---|---|
| `stream_curated_event` | `event_id` | accepted, normalised stream records (no free text; `stated_goal_fingerprint`), with partition, offset and source line |
| `stream_message_log` | `(kafka_topic, kafka_partition, kafka_offset)` | the outcome of every raw-topic message: the consumer's restart state and audit trail |
| `stream_tenant_minute` | `(tenant_id, window_start)` | tumbling-window aggregates |
| `fact_rejected_record` (`channel = 'stream'`) | `(source_file, source_line)` | stream rejects |

---

## 8. Spark (Lab 7)

**Inputs.** The warehouse's Parquet mirror (`output/warehouse/parquet`), which holds the same rows the build loads
into PostgreSQL, plus the partitioned lake (a05) and, over JDBC, the stream tables (metric 9). **Outputs.** One
PostgreSQL table per analysis in schema **`kestrel_analytics`** (written over JDBC, overwrite), and a CSV copy in
[`output/spark/analyses/`](../output/spark/analyses/).

| Id | Analysis (Appendix C) | API | Table |
|---|---|---|---|
| a01 | daily session volume and policy-violation rate by agent/version (joins 3 facts + 2 dims; skew response; explain) | DataFrame | `a01_daily_violation_by_agent_version` |
| a02 | approximate p50/p95 duration by agent type and hour band | DataFrame | `a02_duration_by_agent_type_hour_band` |
| a03 | token, cost and turn distribution by agent/version and tenant | DataFrame | `a03_tokens_cost_turns_by_agent_tenant` |
| a04 | per-session escalation chain: ordered violations with time deltas (**window**) | DataFrame | `a04_session_escalation_chain` |
| a05 | trace-completeness, late-event and rejected-record rate by event type (reads the lake) | DataFrame | `a05_quality_rates_by_event_type` |
| a06 | top five agents by p95 duration, at least 500 terminal sessions | SQL | `a06_top5_agents_p95_duration` |
| a07 | tool categories with the highest failure and denial rates | SQL | `a07_tool_category_failure_denial` |
| a08 | agent versions with the largest week-over-week violation-rate increase (consecutive ISO weeks, ≥ 50 judged calls in both weeks) | SQL | `a08_agent_version_wow_violation_increase` |
| a09 | sessions with the most tool attempts before their terminal event (with the declared budget) | SQL | `a09_sessions_most_tool_attempts` |
| a10 | detection precision/recall by tenant and agent type | SQL | `a10_detection_by_tenant_agent_type` |

Plus `required_metrics` (§9) and `detection_evaluation` (§10).

**Skew.** `ag_004@3.0.2` holds 34.3% of tool calls and `tn_001` 64%. Our explicit response is to broadcast the small
dimensions, aggregate after the join with map-side partial aggregation, and salt the key when a dimension cannot be
broadcast. The plan, the reducer row counts and the effect are in [`spark_plan.md`](spark_plan.md) and
[`skew_report.csv`](../output/spark/skew_report.csv).

**Window function (bonus).** a04 uses `row_number`, `lag` and `min`/`count` over `Window.partitionBy(session_id)`.
`a04_without_window` builds the same table with a self-join (count of earlier violations, max earlier time) and a
group-by. Both directions of `exceptAll` are empty: **24,634 identical rows**
([`window_function_check.json`](../output/spark/window_function_check.json)).

**Independent validation of two analyses.**

* **a01 against pandas.** For 25 randomly sampled (day, agent version) keys, the counts were recomputed in pandas
  straight from the Parquet facts: **25/25 equal**
  ([`validation_a01_vs_pandas.csv`](../output/spark/validation_a01_vs_pandas.csv)).
* **a07 against PostgreSQL.** For 4 sampled tool categories, attempts, failures, denials and both rates were
  recomputed in PostgreSQL SQL against the loaded warehouse, so a different engine and a different copy of the data:
  **4/4 equal** ([`validation_a07_vs_postgres.csv`](../output/spark/validation_a07_vs_postgres.csv)).
* **a06, additionally, against numpy.** Each approximate p95 is checked against numpy's exact percentile, and the
  rank of the approximate value is within the stated error: **5/5**
  ([`validation_a06_vs_numpy_exact.csv`](../output/spark/validation_a06_vs_numpy_exact.csv)).

---

## 9. The eleven required metrics (Spark)

All in [`output/metrics/m2_metrics.csv`](../output/metrics/m2_metrics.csv) and `kestrel_analytics.required_metrics`.
Each row carries its denominator, population filter, late-event policy, null policy and the Spark function used.
**Percentiles:** `percentile_approx(col, p, 10000)`. Spark documents `1.0/accuracy` as the relative error of the
approximation, so the returned value's rank is within 1/10000 = 0.01% of the target. It is always an observed value.
The exact `percentile` (linear interpolation) is reported next to it.

| # | Metric | Value (primary) | Policy in one line | M1 pandas |
|---|---|---|---|---|
| 1 | session volume · success · error | 97,952 · 92.506% · 7.494% | terminal sessions (session.end / .error); accepted rows only | identical |
| 2 | p50 · p95 duration | 17.646 s · 39.538 s (approx); exact 17.646 · 39.560 | terminal sessions; terminal − start event_time | p50 identical; p95 approx −0.022 s, exact identical (see below) |
| 3 | policy-violation · egress-violation rate | 2.169% (6,583 / 303,438) · 17.628% (7,458 / 42,308) | tool calls / flows **with a policy in effect**; variant over all tool calls 2.061% | — |
| 4 | sandbox denial rate · escalation attempts | 3.523% (6,463 / 183,477) · **774 sessions** (877 denied setuid/ptrace/mount) | all decisions; sessions with ≥ 1 denied escalation-class syscall | — |
| 5 | goal-drift · injection-indicator rate | 8.754% (8,748 / 99,935) · 2.506% (10,210 / 407,462) | sessions with expected categories and ≥ 1 tool call; executed ⊄ expected · turns with ≥ 1 indicator / all turns | — |
| 6 | tool failure · retry rate | 8.006% (failure + timeout / executed) · 2.813% | executed = success + failure + timeout (denied never ran); failure-only variant 4.642% · retry_number > 0 / all attempts | — |
| 7 | blast radius · time-to-first-violation | mean 2.345, median 2, max 6 hosts per rogue session (1,562 sessions) · median 4.801 s (exact 4.804 s), p95 8.055 s | rogue = flagged by rule R1 (no labels); destinations = distinct `coalesce(dst_host, dst_ip)` · first indicator turn → first tool / egress violation at or after it | — |
| 8 | trace completeness · correlation coverage | 95.881% · 64.864% (strict 64.172%) | terminal sessions, ≥ 1 turn, no sequence gap · alerts linked to one session at N = 60 s | identical |
| 9 | rejected · late-event rate | 1.384% (activity), 1.164% (4 sources) · 1.499%; **stream** 7.802% · 0.770% | received = catalogue record count; late = lateness > 10 s on accepted rows; stream rates from the Kafka run | identical (batch) |
| 10 | detection precision / recall | **99.69% / 97.75%** (R1, confirmed_rogue vs benign) | §10 | — |
| 11 | estimated LLM cost | **USD 4,055.11** (374,978 of 407,462 turns priced); tn_001 3,211.60 · tn_002 446.62 · tn_003 344.41 · tn_005 41.49 · tn_006 10.99 | price effective at turn time; missing tokens or price: null-excluded (zero-imputation would add +0.79%) | — |

**Where Spark and M1 differ.** Only in the approximate p95 duration: 39.538 s against 39.560 s. pandas
`quantile(..., interpolation="linear")` interpolates at 0-based rank 0.95 × (97,952 − 1) = 93,053.45, between
39.558 s and 39.562 s. `percentile_approx` instead returns one observed duration whose rank may be off by up to
1/10000 × 97,952 ≈ 9.8 positions. It returned 39.538 s, the 93,045th smallest value (rank 0.94990), 9 positions
below. Durations there are 1–4 ms apart, so 9 positions give the −0.022 s. Spark's exact `percentile` uses the same
interpolation as pandas and returns 39.560, identical to M1. Everything else
in metrics 1, 2, 8 and 9 is identical to M1 to the last digit, because the warehouse is built from M1's curated
tables and the session-level definitions are M1's own functions.

**Population choices (declared).**

* **Metric 3.** Records with no policy in effect (`ag_019`) cannot be judged. They are left out of the primary
  denominator and never counted as violations; the variant over all rows is reported.
* **Metric 6.** "Completed attempts" excludes `denied` calls, which never ran. A `timeout` counts as a failure.
* **Metric 7.** "Rogue session" is the detection rule's output, because labels are held out. Distinct destinations
  come from flows (tool-initiated egress). LLM gateway calls are not flows (Dataset Description 3.6).
* **Missing tokens.** Null-excluded (M1 policy). The effect is in `M1/output/metrics/missing_token_effect.csv`.

---

## 10. Detection rules and their evaluation (metric 10, a10)

`spark/detection.py`. Each rule reads only observable signals from `fact_session`. The rules were fixed from the
project's scenario before the labels were opened, and **the labels are read in one place, at the end**
(`detection.evaluate`). No rule, feature or threshold reads a label.

| Rule | Definition |
|---|---|
| **R1** injection → violation (primary) | a prompt turn with ≥ 1 injection indicator, and a tool-policy or egress violation at or after that turn |
| R2 injection → violation + drift | R1, and the executed tool categories leave the task's expected set |

**Scoring policy.** Positive = `confirmed_rogue`, negative = `benign`. `suspicious` sessions (1,876) are uncertain
and left out of the primary score, and a variant counts them as positive. Only labelled sessions can be scored
(6,037 labels, all on warehouse sessions), so precision is measured on the labelled sample.

| Rule | Labels | TP | FP | FN | TN | Precision | Recall | F1 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| R1 | confirmed_rogue vs benign | 1,305 | 4 | 30 | 2,822 | **99.69%** | **97.75%** | 0.987 |
| R1 | + suspicious as positive | 1,329 | 4 | 1,882 | 2,822 | 99.70% | 41.39% | 0.585 |
| R2 | confirmed_rogue vs benign | 1,301 | 2 | 34 | 2,824 | 99.85% | 97.45% | 0.986 |
| R2 | + suspicious as positive | 1,312 | 2 | 1,899 | 2,824 | 99.85% | 40.86% | 0.580 |

Reading. The scenario's chain (suspicious content, then an action outside the sandbox policy) separates confirmed
rogue sessions from benign ones almost perfectly. R2's drift condition removes 2 false positives at the cost of 4
missed rogue sessions (recall −0.3 points). `suspicious` sessions mostly do *not* show the chain, which is consistent with them being uncertain, so
the broad variant's recall drops. The rule flags 1,562 of 100,000 sessions (1.56%), in line with the documented
~1.5% rogue rate.

---

## 11. Changes to M1 conventions

| M1 statement | M2 |
|---|---|
| "the M2 consumer reuses `clean_activity(watermark_s=60)`", which is lateness-based (`ingest − event > 60 s`) | Replaced by a real **event-time watermark** (§7.4). A lateness rule would judge by arrival time, while the Dataset Description puts watermarks on `event_time`. |
| `ORPHAN_EVENT` = no accepted start in the batch | On the stream, orphans are decided only when the watermark or `EOS` makes a later start impossible (§7.3) |
| Dedup keeps the earliest `ingest_time` | Unchanged. On the stream this is the first copy read, and a redelivery of the same offset is not a duplicate. |

New quality flags: `RAW_TEXT_DROPPED` (stream; a message still carried `stated_goal`). New reject provenance columns
in `fact_rejected_record`: `source_file_id`, `zone`, `channel`, `detected_by`, `kafka_topic`, `kafka_partition`,
`kafka_offset`.
