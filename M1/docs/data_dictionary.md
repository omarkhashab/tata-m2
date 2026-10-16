# Kestrel M1 · Data dictionary

This document describes the curated output of the M1 pipeline (`python -m kestrel_m1.pipeline`, or the `pipeline`
service in `docker compose`) and the policies used to build it. Problems found in the data, how each was detected
and the decision taken are in [`dq_findings.md`](dq_findings.md).

Every table is written as Parquet to `output/curated/<table>.parquet` and loaded into PostgreSQL schema **`kestrel`**.
Code references point into `kestrel_m1/`.

---

## 1. Sources

| File | Format | How it is read | Records | Clock |
|---|---|---|---|---|
| `agent_activity.parquet` | Parquet | `pyarrow`, schema checked; timestamps kept as raw strings | 937,752 rows | UTC (several string formats) |
| `network_flows.csv` | CSV | every column read as text, then typed | 42,392 rows | UTC |
| `sandbox_audit.log` | syslog text | anchored regex per line | 184,005 lines | host-local UTC+2, no year, per-host skew |
| `siem_alerts.json` | nested JSON | envelope + `json_normalize` of `destination` / `technique` | 3,754 alerts | ISO-8601 with an explicit offset |
| `tenants.csv`, `agents.csv`, `tools.csv`, `model_pricing.csv` | CSV | text, then typed | 6 / 22 / 18 / 5 rows | dates (UTC) |
| `sandbox_policies.json` | nested JSON | one row per (profile, version), lists kept as lists | 10 versions | dates (UTC) |

`stream_events.json` and `incident_labels.csv` are **not used in M1**. The labels are held-out ground truth for M2
precision/recall and never feed any rule or feature.

**Provenance.** Every record carries `source_file` and `source_line` from the moment it is read
(`readers.py`). For text files, `source_line` is the 1-based physical line where the record starts. For Parquet it is
the 1-based row number, and for the pretty-printed JSON alert array the line that holds the alert's `alert_id`. Raw files are committed
with `-text` in `.gitattributes`, so line numbers survive a clone on any OS.

---

## 2. Policies

### 2.1 Timezone policy

Every timestamp in the curated output is **tz-aware UTC** (`datetime64[ms, UTC]`, `timestamptz` in Postgres). The
sources carry at most millisecond precision.

| Source / field | Convention found | How it was established | Conversion |
|---|---|---|---|
| activity `ingest_time`, `finished_at` | ISO-8601 with `Z` | format shapes: one shape only | parse as UTC |
| activity `event_time` | UTC in four string formats: ISO with ms, ISO without ms, epoch **milliseconds** (13 digits), `dd/mm/yyyy HH:MM:SS` | for every format, `ingest_time − event_time` is about 1 s with no whole-hour offset, so all are UTC. A day field > 12 in slash dates proves day-first. 13-digit epochs land in the pack window only as ms. | parse each format as UTC; non-ISO rows flagged `NONSTANDARD_TIMESTAMP_FORMAT`; unparseable values rejected `MALFORMED_TIMESTAMP` |
| flows `flow_time` | genuine UTC | a flow trails its causing `tool.call` by a median +0.34 s, with no hour offset | as-is; flows are the clock anchor for the audit log |
| audit `log_time` (`Mon DD HH:MM:SS`) | host-local **UTC+2 on every host**, no DST, no year, plus a per-host skew | pairing `socket_connect` lines with UTC flows (same session and target) gives a per-host median offset of 1.97 to 2.03 h, whatever region the hostname names. Pairing every decision with the accepted tool call that caused it covers all 42 hosts and agrees with the flow pairing within 0.4 s. The `Apr` lines are the +2 h spill-over of 31 March evening. | year = 2026; `utc = local − measured per-host offset` (UTC+2 plus a skew of −90 to +87 s, re-measured on every run, table `audit_host_offsets`); a host with no pair falls back to exactly UTC+2 (flag `CLOCK_OFFSET_FALLBACK_UTC_PLUS_2`, not needed in this pack) |
| SIEM `detected_at` | ISO-8601 with an offset that differs per `source_system`: corp-siem `+02:00`, egress-proxy `+03:00`, cloud-ids `−05:00` | after conversion, every system's alerts trail their flows by 0 to 45 s, so the offsets are honest | convert with the stated offset; the original string is kept in `detected_at_raw` and the offset in `utc_offset` |
| reference dates (`launch_date`, `valid_from`, `effective_from`, ...) | dates or ISO strings | — | midnight UTC |

After correction, audit lines line up with their flows to within −0.68 s … +0.39 s (p01 to p99).

### 2.2 Deduplication policy (natural keys)

| Table | Natural key | Rule |
|---|---|---|
| agent activity | `event_id` | at-least-once delivery: keep the copy with the **earliest `ingest_time`** (source line breaks ties); later copies are rejected `DUPLICATE_EVENT_ID` with the kept line in `reason_detail` |
| network flows | `flow_id` | first occurrence in file order wins (no duplicates in this pack) |
| sandbox audit log | the whole raw line (the syslog has no id) | an exact repeat of an earlier line is a duplicate delivery: rejected `DUPLICATE_EVENT_ID`. The accepted line's id is `L<line number>` |
| SIEM alerts | `alert_id` | first occurrence wins (no duplicates in this pack) |
| agents | `(agent_id, release_version)` after stripping a leading `v` | keep the canonical (un-prefixed) row; the dropped row goes to `entity_resolution` |
| tools | `(tool_id, tool_version)` | identical duplicate row dropped, logged in `entity_resolution` |
| tenants | `tenant_id`, then same company | see 2.3 |
| policies | `(sandbox_profile_id, policy_version)` | unique |
| pricing | `(model_id, effective_from)` | unique |

### 2.3 Entity-resolution policy

One real thing under several identifiers is mapped to a single canonical key. Every mapping is written to table
**`entity_resolution`** (source file, line, raw key, resolved key, rule), and every repaired activity row carries a
quality flag. Nothing is merged silently.

| Entity | Raw keys | Canonical key | Rule |
|---|---|---|---|
| tenant | `tn_004` "nilebank" | `tn_003` "NILEBANK Ltd." | same normalised name, identical security contact and onboarding time. Activity rows on `tn_004` are rewritten (`TENANT_ALIAS_RESOLVED`); there are none in this pack |
| agent version | `v2.3.1`, `v2.2.0` | `2.3.1`, `2.2.0` | strip a leading `v` (`VERSION_PREFIX_STRIPPED`). The duplicate `ag_007,v2.3.1` row in `agents.csv` is dropped in favour of the `2.3.1` row; its conflicting name/autonomy are recorded |
| tool | names `web_fetch`, `http_get`, `webfetch` used as `tool_id` | `tl_web_fetch` | alias map name → id built from `tools.csv` (`TOOL_ALIAS_RESOLVED`) |
| tool call status | `succeeded` | `success` | unambiguous synonym (`STATUS_NORMALISED`) |
| flow destination | empty `dst_host` | host seen for the same `dst_ip` on other flows | only IP→host pairs that are unambiguous in the data (`DST_HOST_FILLED`); otherwise left empty and the IP is the target (`DST_HOST_UNKNOWN`) |

**Versioned dimensions (Type-2).** `dim_agent` has one row per `(agent_id, release_version)` with a surrogate
**`agent_sk`** and a validity window `[valid_from_utc, valid_to_utc)`: `valid_from` = `launch_date`, `valid_to` = the
next version's launch. A record gets the `agent_sk` of its *own* `(agent_id, release_version)`; if its event time lies
outside that version's window it is kept and flagged `AGENT_VERSION_OUTSIDE_WINDOW`. Tools are keyed on
`(tool_id, tool_version)`, and the category can differ per version (`CATEGORY_DIFFERS_ACROSS_VERSIONS`, kept per
version).

### 2.4 Policy-version (validity-window) policy

A record is judged against the policy version **whose effective window `[valid_from, valid_to)` contains the record's
own UTC time**, never the current version (`reference.attach_policy`, an as-of join). The profile comes from the
agent version (`dim_agent.sandbox_profile_id`).

* **Overlap rule: the newer version wins.** `sp_research_std` v2 (declared to 2026-02-01) overlaps v3 (from
  2026-01-25). Evidence: the sandbox's own audit log reports `policy_version = 3` on all 346 research decisions inside
  the overlap. v2's effective window is cut to 2026-01-25 (`WINDOW_CUT_BY_NEWER_VERSION`; the declared end is kept in
  `valid_to_declared_utc`).
* **No network block = no egress.** `sp_ops_elevated` v4 has no `network` block, so every flow under it is a violation
  (`NO_NETWORK_BLOCK_NO_EGRESS`, risk token `no_egress_allowed`).
* **No policy in effect.** `ag_019` points to `sp_missing_profile`, which does not exist. Its records get
  `policy_version_applied` = null, `is_policy_violation` = null and flag `NO_POLICY_IN_EFFECT`: a violation is not
  invented for a missing policy.
* **Cross-check.** Where the audit log reports a policy version, it equals the window join on 100% of lines. The
  `-` lines are exactly ag_019's (`POLICY_VERSION_NOT_LOGGED`).

### 2.5 Policy-violation definition (`is_policy_violation`)

| activity_type | Violation when | Null when |
|---|---|---|
| `tool_call` | `tool_id` not in the effective policy's `allowed_tools` | no policy in effect |
| `network_flow` | the policy has no network block; or the port is in `denied_ports`; or the destination host matches **no** allowlist entry. Wildcards are matched as patterns: `*.public-data.example` matches exactly one extra DNS label, case-insensitively, not the apex and not deeper names. An IP-only destination cannot be shown to be allowlisted, so it counts as a violation (deny by default). | no policy in effect |
| `sandbox_decision` | the sandbox said `DENY`; or a `socket_connect` breaks the egress rule above although the sandbox said `ALLOW` (the sandbox fails open on `NO_NETWORK_POLICY` and `WILDCARD_MATCH_UNVERIFIED`; we judge by the policy, not by the sandbox) | no policy in effect |
| `lifecycle`, `prompt_turn` | — | always (no policy rule applies) |
| `siem_alert` | — | always (the alert is the victim's view; its flow carries the violation) |

### 2.6 Null policy

* **Required fields** (Dataset Description 3.5, "filled on"): a null in a field required for the row's
  `event_type` is a reject `MISSING_REQUIRED_FIELD`. Example: `status` null on a turn or tool call.
* **Optional or type-specific fields** stay null where the event type does not fill them (e.g. `tool_id` on an
  `llm.turn`). In the canonical table, the columns that do not apply to an activity type are null (see section 3).
* **`output_tokens` is deliberately not required** (missing-token policy, 2.9): the turn is kept with a null count.
* **Null markers** in text sources become real nulls: the audit log's `-`, empty CSV cells, empty strings in JSON.
* **Invalid enum values in reference data** become null with a flag (tenant region `eu-central-9` →
  `INVALID_ENUM_REGION`).
* **Pipe-separated list columns** (`quality_flags`, `risk_indicators`) are never null: "none" is the empty string `''`.
  Null and empty stay distinct, also in Postgres.
* **Nulls are never imputed** with 0, a mean, or the "current" dimension version.

### 2.7 Reject policy

A record that cannot be trusted goes to **`fact_rejected_record`** with its raw payload, source file, line and
**exactly one** reason from the closed enum. When several reasons apply, the **first in this order** wins
(`rejects.REASONS`, `rejects.first_reason`):

| # | Reason | Applied to | Rule in this pipeline |
|---|---|---|---|
| 1 | `DUPLICATE_EVENT_ID` | activity, audit | later copy of an `event_id` (activity) or of an identical line (audit) |
| 2 | `UNKNOWN_AGENT_ID` | activity | `(agent_id, release_version)` not in `agents.csv` after normalisation |
| 3 | `UNKNOWN_TENANT_ID` | activity | tenant not in `tenants.csv` after alias resolution |
| 4 | `MALFORMED_TIMESTAMP` | activity, flows, audit, SIEM | timestamp present but unparseable or impossible (`not-a-time`, `2026-02-30`); SIEM additionally requires an explicit offset |
| 5 | `ORPHAN_EVENT` | activity, flows, audit | a non-start record whose session has no **accepted** `session.start`. Start rows are judged on every other reason first, then orphans are computed, so a rejected start also orphans its session |
| 6 | `INVALID_EVENT_TYPE` | activity | `event_type` outside the five-value enum |
| 7 | `MISSING_REQUIRED_FIELD` | all | required field null (`-` in the audit log) |
| 8 | `INVALID_NUMERIC_RANGE` | activity, flows, SIEM | negative counts/latency/tokens, `retry_number < 0`, `sequence_number < 1`, port outside 1..65535, confidence outside 0..1 |
| 9 | `LATE_BEYOND_WATERMARK` | (stream only) | not applied in batch; the M2 consumer passes a 60 s watermark |
| 10 | `UNPARSEABLE_RECORD` | audit | line does not match the syslog regex (truncated) |
| 11 | `SCHEMA_VIOLATION` | all | value outside the contract: enum (status per event type, protocol, verdict, severity, decision, action), id pattern, IP format, non-integer number |

Invariant, asserted on every run: **accepted + rejected = received** for every source file.

### 2.8 Late-event policy

* `lateness_s = ingest_time_utc − event_time_utc` (activity only; the other sources have no arrival time).
  **Late** = `lateness_s > 10`.
* **Batch: accept and flag `LATE`.** No watermark is applied (`clean_activity(watermark_s=None)`), so the late-event
  rate can be measured and late rows still fill their sequence slot. Watermarks are always on `event_time`; the M2
  Kafka consumer reuses the same code with `watermark_s=60`, which rejects `LATE_BEYOND_WATERMARK`.

### 2.9 Missing-token and cost policy

* **Missing `output_tokens`: null-excluded.** The turn is kept, `tokens_out` and `cost_usd` stay null and are left out of
  sums and averages. They are never set to 0, because a turn that produced text did not produce zero tokens. Flag
  `OUTPUT_TOKENS_MISSING`. Effect: zero-imputation would raise the estimated total cost by 0.79% and lower mean output
  tokens per turn by 1.0% (`output/metrics/missing_token_effect.csv`).
* **Cost** (prompt turns only) = `tokens_in/1000 × input_price + tokens_out/1000 × output_price`, at the price
  **effective at the turn's time** (as-of join on `effective_from`; `effective_to` derived as the next
  `effective_from` of the same model), in USD rounded to 6 decimals.
* **No price** (`mdl_kst_nano_1` has empty prices, and some nano turns precede its first `effective_from`):
  `cost_usd` null, flag `PRICE_MISSING`.

### 2.10 Correlation policy (SIEM ↔ network flows)

Alerts have no `session_id`. An alert is linked to a flow by what both sides saw (`correlate.py`):

```
alert.source_ip        = flow.src_ip
alert.destination.host = flow.dst_ip      and   alert.destination.port = flow.dst_port
|alert.detected_at_utc − flow.flow_time_utc| ≤ N,   N = 60 s
```

* **Why N = 60 s:** matches grow until 45 s, the longest detection lag observed, and the counts are **identical for
  every N from 45 s to 120 s**. 60 s sits inside that plateau with a margin. Larger N only adds alerts whose nearest
  flow is 155 to 393 s away; at 3,600 s ambiguity doubles. Full table: `correlation_sensitivity`.
* **Outcome per alert:** `matched` (exactly one candidate flow), `ambiguous` (two or more), or `unmatched` (none).
  Result: **2,409 matched / 27 ambiguous / 1,318 unmatched**.
* **Session of an alert:** the matched flow's session. For an ambiguous alert, the session is recorded only when every
  candidate flow belongs to the same session (26 of 27, `session_resolution = single_session`); the flow itself stays
  undecided. Every candidate pair is kept in `siem_correlation_candidates`.
* **Never forced:** the 1,251 cloud-ids `*_external` alerts come from source IPs that no Kestrel sandbox ever used, so
  they stay unmatched (off-platform activity).

### 2.11 Quality flags

Non-fatal issues are kept and flagged in the pipe-separated `quality_flags` column, never silently fixed.

| Flag | Table(s) | Meaning |
|---|---|---|
| `NONSTANDARD_TIMESTAMP_FORMAT` | activity | `event_time` was epoch-ms or `dd/mm/yyyy`; parsed as UTC |
| `VERSION_PREFIX_STRIPPED` | activity, dim_agent | `v` prefix removed from `release_version` |
| `TENANT_ALIAS_RESOLVED` | activity | tenant id rewritten to its canonical tenant |
| `TOOL_ALIAS_RESOLVED` | activity | tool name used as `tool_id`, resolved to the id |
| `STATUS_NORMALISED` | activity | `succeeded` → `success` |
| `SUCCESS_WITH_ERROR_CLASS` | activity | tool call `success` but `error_class` filled; kept as received |
| `LATE` | activity | `lateness_s > 10` |
| `OUTPUT_TOKENS_MISSING` | activity | `llm.turn` with null `output_tokens` |
| `FINISHED_BEFORE_START` | activity | tool call `finished_at` earlier than its `event_time`; kept, not used by any metric |
| `AGENT_VERSION_OUTSIDE_WINDOW` | activity | event time outside the agent version's validity window |
| `TENANT_DIFFERS_FROM_AGENT` | activity | row's tenant differs from the agent's owning tenant |
| `NO_POLICY_IN_EFFECT` | activity, flows, audit | no policy version covers the record (ag_019) |
| `PRICE_MISSING` | activity, dim_model_price | no price effective at turn time |
| `DST_HOST_FILLED` / `DST_HOST_UNKNOWN` | flows | empty `dst_host` filled from the IP map / still unknown |
| `POLICY_VERSION_NOT_LOGGED` | audit | audit line had `-` as policy version |
| `POLICY_VERSION_DERIVED` / `POLICY_VERSION_MISMATCH` | audit | version filled by the window join / reported version ≠ window join (none in this pack) |
| `CLOCK_OFFSET_FALLBACK_UTC_PLUS_2` | audit | host had no pair to measure its skew |
| `INSTANCE_DIFFERS_FROM_SESSION` | audit | `sandbox_instance_id` differs from the session's |
| `CORRELATION_UNMATCHED` / `CORRELATION_AMBIGUOUS` / `CORRELATION_AMBIGUOUS_FLOW_SINGLE_SESSION` | canonical (SIEM rows) | correlation outcome |
| `INVALID_ENUM_REGION`, `MISSING_SECURITY_CONTACT` | dim_tenant | reference-data issues |
| `NO_POLICY_PROFILE`, `UNKNOWN_MODEL`, `NON_SEMVER_VERSION` | dim_agent | reference-data issues |
| `CATEGORY_DIFFERS_ACROSS_VERSIONS` | dim_tool | same tool, different category per version |
| `EFFECTIVE_TO_DERIVED` | dim_model_price | `effective_to` derived from the next price |
| `WINDOW_CUT_BY_NEWER_VERSION`, `GAP_BEFORE_NEXT_VERSION`, `NO_NETWORK_BLOCK_NO_EGRESS` | dim_policy | overlap resolved / window gap / no egress allowed |

### 2.12 Risk indicators

`risk_indicators` is the pipe-separated union of signals on a record (empty string = none):

* **prompt turns:** the detector's `injection_indicators` (`instruction_override`, `role_confusion`, `exfil_request`,
  `encoded_payload`, `tool_coercion`, `policy_probe`). This is noisy detector output, not ground truth.
* **tool calls:** `tool_not_allowed`, `tool_denied`
* **flows:** `no_egress_allowed`, `port_denied`, `egress_not_allowlisted`, `egress_host_unknown`, `egress_blocked`
* **sandbox decisions:** `sandbox_denied` plus the lower-cased sandbox reason (`egress_not_allowed`,
  `path_out_of_scope`, `escalation_forbidden`, ...), `allowed_despite_policy` (fail-open), `escalation_attempt`
  (`setuid` / `ptrace` / `mount`)
* **SIEM alerts:** `siem_alert`, `technique:<MITRE id>`. SIEM `severity` is the target system's opinion and is carried
  in `stg_siem_alerts` unchanged, never re-scored.

---

## 3. `canonical_activity` (Dataset Description §5)

Grain: **one row per accepted record** from the four event sources. 1,154,314 rows, sorted by
`(event_time_utc, activity_id)`. PK `activity_id`; indexes on `session_id`, `event_time_utc`, `activity_type`.

| Column | Type (Postgres) | Description | Null when |
|---|---|---|---|
| `activity_id` | text, PK | source key with a source prefix: `act:<event_id>`, `flow:<flow_id>`, `audit:L<line>`, `siem:<alert_id>` | never |
| `source_file` | text | raw file the record came from | never |
| `source_format` | text | `parquet`, `csv`, `syslog`, `nested_json` | never |
| `source_line` | bigint | 1-based line (row number for Parquet) in `source_file` | never |
| `event_time_utc` | timestamptz | when it happened, UTC (activity `event_time`, flow `flow_time`, audit `log_time` corrected, SIEM `detected_at`) | never |
| `ingest_time_utc` | timestamptz | when the platform received it | flows, audit, SIEM (no arrival time in those sources) |
| `lateness_s` | double | `ingest_time_utc − event_time_utc` in seconds | as `ingest_time_utc` |
| `tenant_id` | text | canonical tenant (aliases resolved). Flows/audit/SIEM inherit it from their session | SIEM alerts without a session (35%) |
| `agent_sk` | bigint | `dim_agent` surrogate key of the agent version | as `tenant_id` |
| `session_id` | text | session; for SIEM alerts, the session found by correlation | as `tenant_id` |
| `task_id` | text | task of the session (from its `session.start`) | as `tenant_id` |
| `activity_type` | text | `lifecycle`, `prompt_turn`, `tool_call`, `network_flow`, `sandbox_decision`, `siem_alert` | never |
| `activity_subtype` | text | activity `event_type` (`session.start` / `.end` / `.error`, `llm.turn`, `tool.call`); flow `protocol`; audit `action`; SIEM `rule` | never |
| `status` | text | the source's own outcome: `ok`/`error` (lifecycle, turns), `success`/`failure`/`denied`/`timeout` (tool calls), the sandbox reason (`EGRESS_ALLOWED`, `PORT_DENIED`, ...), the correlation status (`matched`/`ambiguous`/`unmatched`) for alerts | flows (no status in the source) |
| `verdict` | text | one access vocabulary: `allowed`/`denied` (tool calls: `denied` iff status is `denied`; sandbox `ALLOW`/`DENY`), `allowed`/`blocked`/`logged_only` (flows, as written) | lifecycle, prompt turns, SIEM |
| `target_resource` | text | tool call target (host, table, path or command); flow `host:port` (IP if host unknown); audit target; SIEM `host:port` | lifecycle, prompt turns |
| `policy_version_applied` | bigint | version of the session's sandbox policy effective at `event_time_utc` (2.4) | no policy in effect (ag_019); SIEM alerts |
| `is_policy_violation` | boolean | see 2.5 | lifecycle, prompt turns, SIEM, no policy in effect |
| `risk_indicators` | text | pipe-separated union (2.12); `''` = none | never |
| `tokens_in` | bigint | input tokens (prompt turns only) | every non-turn row |
| `tokens_out` | bigint | output tokens (prompt turns only) | non-turn rows; turns with missing `output_tokens` (1.0% of turns) |
| `cost_usd` | double | estimated LLM cost at the price effective at turn time (2.9) | non-turn rows; turns without output tokens or without a price (8.0% of turns) |
| `quality_flags` | text | pipe-separated non-fatal issues (2.11); `''` = none | never |

Row counts: prompt_turn 407,462 · tool_call 319,361 · lifecycle 197,952 · sandbox_decision 183,477 ·
network_flow 42,308 · siem_alert 3,754. Event time range: 2026-01-31 00:00:33 to 2026-03-31 23:54:31 UTC.

---

## 4. `fact_rejected_record`

Grain: **one row per rejected source record**. 13,589 rows. PK `reject_id`; index on `(source_file, source_line)` and `reason`.

| Column | Type | Description |
|---|---|---|
| `reject_id` | bigint, PK | sequence over `(source_file, source_line)` |
| `source_file` | text | raw file |
| `source_format` | text | `parquet`, `csv`, `syslog`, `nested_json` |
| `source_line` | bigint | 1-based line / row number of the record in the raw file; unique per file |
| `record_id` | text | the record's own id (`event_id`, `flow_id`, `alert_id`); null for audit lines (the syslog has no id) and when the id itself is missing |
| `reason` | text | exactly one value of the closed enum (2.7) |
| `reason_detail` | text | human-readable why, e.g. `copy kept at line 618700 (earliest ingest_time)`, `null: status`, `below minimum: input_tokens`, `identical to line 12715 (no record id: whole line is the key)` |
| `raw_payload` | text | the record as received: a JSON object of the raw fields (activity; flows with every CSV cell as text), the original nested alert JSON (SIEM), or the raw syslog line (audit) |

Counts by reason: DUPLICATE_EVENT_ID 7,431 · INVALID_NUMERIC_RANGE 1,548 · UNKNOWN_AGENT_ID 1,332 ·
UNKNOWN_TENANT_ID 730 · MISSING_REQUIRED_FIELD 730 · MALFORMED_TIMESTAMP 708 · UNPARSEABLE_RECORD 524 ·
INVALID_EVENT_TYPE 302 · ORPHAN_EVENT 284.

---

## 5. Other curated tables

| Table | Grain / key | Purpose |
|---|---|---|
| `stage_summary` | (source_file, outcome) | received records per source split into ACCEPTED and each reject reason, with share % |
| `stg_agent_activity` | `event_id` | accepted activity rows with every typed, resolved and derived column (raw values kept as `*_raw`), policy, risk, tokens, prices and cost |
| `stg_network_flows` | `flow_id` | accepted flows, `dst_host` repaired, session attributes inherited, `violation_rule` |
| `stg_sandbox_audit` | `audit_id` (`L<line>`) | accepted audit lines with UTC time, host offset used, reported vs applied policy version |
| `stg_siem_alerts` | `alert_id` | flattened alerts with `detected_at_utc`, raw string and offset, `severity` as received |
| `session` | `session_id` | one row per session with an accepted `session.start`: tenant, agent version, `agent_sk`, sandbox profile/instance, task, start time. The parent for orphan checks |
| `siem_correlation` | `alert_id` | correlation outcome per alert: status, candidate counts, flow, session, Δt, `nearest_key_match_s` |
| `siem_correlation_candidates` | (alert_id, flow_id) | every alert–flow pair within N |
| `correlation_sensitivity` | `n_s` | matched / ambiguous / unmatched / with-session counts for N = 5 s to 3,600 s |
| `audit_host_offsets` | `host` | measured clock offset per audit host (pairs, median, p05, p95, skew vs UTC+2) |
| `dim_tenant` | `tenant_id` | 5 canonical tenants (tn_004 merged into tn_003) |
| `dim_agent` | `agent_sk` (natural key `agent_id, release_version`) | 21 agent versions, Type-2 with validity window and `is_current` |
| `dim_tool` | `(tool_id, tool_version)` | 17 tool versions |
| `dim_policy` | `(sandbox_profile_id, policy_version)` | 10 policy versions, flattened, effective and declared windows; list columns are pipe-separated in Postgres |
| `dim_model_price` | `(model_id, effective_from_utc)` | 5 prices with derived `effective_to_utc` |
| `entity_resolution` | (entity, source_file, source_line) | every merged / dropped reference row and alias, with its rule |

## 6. Metrics

Appendix A requires the denominator, population filter, late-event policy and null policy of every metric. They
are stated below. The same text is the `POLICIES` table in `kestrel_m1/metrics.py`, which also writes it to
`output/metrics/m1_metric_policies.csv`. The values are in `output/metrics/m1_metrics.csv`, and
`notebooks/02_metrics.ipynb` shows how each one is computed.

### 6.1 Metric 1: session volume / success / error rate

* **Denominator:** distinct session_id with an accepted session.end or session.error row
* **Population filter:** accepted activity rows (after dedup and rejects). Every terminal session has an accepted session.start because orphans are rejected. Sessions with no terminal row are excluded.
* **Late-event policy:** late rows are accepted and only flagged (the batch has no watermark), so a late terminal row still makes its session terminal
* **Null policy:** none possible: event_type, session_id and event_time are required fields, and rows missing them are rejected

### 6.2 Metric 2: p50 / p95 session duration

* **Denominator:** terminal sessions (same population as metric 1)
* **Population filter:** duration = terminal event_time_utc - session.start event_time_utc; pandas Series.quantile with linear interpolation between closest ranks
* **Late-event policy:** included. Lateness concerns ingest_time, and the duration uses event_time only
* **Null policy:** a session without an accepted start would have no duration and be excluded; the curated data has none. latency_ms is never used as the duration, only as a cross-check (disagreement > 1 s reported)

### 6.3 Metric 8a: trace completeness

* **Denominator:** terminal sessions (same population as metric 1)
* **Population filter:** complete = at least one accepted llm.turn, and the accepted sequence_numbers are exactly 1..max (no gap, no repeat). A row rejected for any reason except DUPLICATE_EVENT_ID (which leaves one accepted copy) leaves a gap, so the session counts as incomplete.
* **Late-event policy:** late rows are accepted, so they fill their sequence slot
* **Null policy:** sequence_number is required; rows without it are rejected, which leaves a gap

### 6.4 Metric 8b: correlation coverage

* **Denominator:** all accepted SIEM alerts
* **Population filter:** covered = the alert is linked to exactly one session at N = 60 s: matched (one candidate flow) plus ambiguous alerts whose candidate flows all belong to one session. Ambiguous alerts spanning several sessions, and unmatched alerts, are not covered. Strict (matched only) and on-platform variants are reported next to it.
* **Late-event policy:** not applicable: alerts and flows carry no ingest_time; |delta t| <= N is the only time rule
* **Null policy:** an alert without a session is not covered; it is never force-matched

### 6.5 Metric 9a: rejected-record rate

* **Denominator:** received records per source = accepted + rejected (every received record lands in exactly one of the two; the pipeline asserts this)
* **Population filter:** the four event sources. Headline = agent_activity.parquet; the overall rate is pooled over all four. Reference files are cleaned by dedup and repair, not rejected, so they are not counted.
* **Late-event policy:** late rows are not rejected in batch (no LATE_BEYOND_WATERMARK), so lateness does not raise this rate
* **Null policy:** a null in a required field is a reject (MISSING_REQUIRED_FIELD) and is counted

### 6.6 Metric 9b: late-event rate

* **Denominator:** accepted agent_activity rows
* **Population filter:** late = ingest_time_utc - event_time_utc > 10 s. Only agent_activity has an ingest_time, so the other sources have no lateness. Late duplicate copies were already removed by dedup (the earliest ingest is kept) and are not counted.
* **Late-event policy:** this metric is the late policy's measurement: late rows are kept and flagged LATE
* **Null policy:** none possible: both timestamps are required and a malformed one is a reject

### 6.7 Missing token counts

* **Denominator:** llm.turn rows
* **Population filter:** turns whose output_tokens is null
* **Late-event policy:** -
* **Null policy:** null-excluded: the count stays null, cost_usd is null and both are left out of sums and averages, never imputed as 0 (effect shown by missing_token_effect)
