# Kestrel M1 · Data-quality findings

Every problem found in the batch files: **what** it is, **how it was detected**, how many records it touched, the
**decision** taken, and what it became in the curated output. The policies the decisions follow (timezone, dedup,
entity resolution, nulls, rejects, late events, correlation) are stated in [`data_dictionary.md`](data_dictionary.md).

* **Detection** was done in `notebooks/01_profiling.ipynb` **before** any cleaning. The finding IDs match its
  findings log, `output/profiling/findings.csv`.
* **Decisions** are implemented in `kestrel_m1/` (module in brackets).
* **Two counts per finding.** *Found* is what profiling counted on the raw file. *Curated* is the final effect after
  the pipeline. They can differ for two reasons:
  * **One reason per reject:** a reject gets only its first reason in enum order, so a row that also fails an earlier
    check (e.g. it is a duplicate) is counted under that earlier reason.
  * **Dedup runs first:** an issue on a rejected duplicate copy does not reach the accepted rows.

Totals: 1,167,903 event records received → **1,154,314 accepted** into `canonical_activity`, **13,589 rejected** into
`fact_rejected_record`.

---

## 1. Reference data (`reference.py`)

| ID | Problem | How detected | Found | Decision | Curated |
|---|---|---|---|---|---|
| T1 | `tn_003` "NILEBANK Ltd." and `tn_004` "nilebank" are the same company | normalised name match, plus identical `security_contact` and `onboarded_at` | 2 rows | entity-resolve `tn_004` → `tn_003` | `dim_tenant` has 5 tenants; mapping in `entity_resolution`; no activity used `tn_004` |
| T2 | tenant region `eu-central-9` is not a valid region | enum check | 1 | set `region` null, keep the raw value in `region_raw` | flag `INVALID_ENUM_REGION` on `tn_005` |
| T3 | `security_contact` missing | null check | 1 | keep null (nullable attribute) | flag `MISSING_SECURITY_CONTACT` |
| T4 | `tn_006` is inactive (`is_active = false`) but has traffic | join tenants to activity | 15,043 rows | keep: inactive now does not mean it never ran | 14,882 accepted rows |
| AG1 | `ag_007,v2.3.1` duplicates `ag_007,2.3.1` with a different name and autonomy level | strip the `v`, then check for a duplicate natural key | 1 | drop the `v`-prefixed row, keep the canonical row's attributes | 21 rows in `dim_agent`; conflict recorded in `entity_resolution` |
| AG2 | `ag_019` points to `sp_missing_profile`, which does not exist | FK check against the policies | 1 agent | keep the agent; no policy in effect for its records | `NO_POLICY_PROFILE` on the agent; 49,788 activity rows + 2,446 audit lines flagged `NO_POLICY_IN_EFFECT`, `is_policy_violation` null |
| TL1 | `tl_vector_search 2.0.1` listed twice | `duplicated()` on all columns | 1 | dedup on `(tool_id, tool_version)` | 17 rows in `dim_tool`; logged in `entity_resolution` |
| TL2 | `tl_web_fetch` is named `web_fetch` / `http_get` / `webfetch` across versions | `nunique(tool_name)` per `tool_id` | 3 names | alias map tool name → `tool_id` | used by AC9; 3 rows in `entity_resolution` |
| TL3 | `tl_code_exec` category is `code_exec` (0.9.1) but `shell` (0.9.0) | `nunique(tool_category)` per `tool_id` | 1 tool | keep the category per version (versioned dimension) | flag `CATEGORY_DIFFERS_ACROSS_VERSIONS` |
| P1 | `mdl_kst_nano_1` has empty input and output prices | null check | 1 model | no cost without a price; never guessed | `cost_usd` null + `PRICE_MISSING` on all 28,663 accepted nano turns |
| P2 | `effective_to` is never filled | null check | 5 rows | `effective_to` = next `effective_from` of the same model (open-ended for the last) | flag `EFFECTIVE_TO_DERIVED`; `mdl_kst_pro_1` changes price on 1 March |
| P3 | nano turns occur before nano's first `effective_from` (2026-02-01) | first turn per model vs `effective_from` | 451 (ISO-format rows only) | no price is effective at turn time → cost null | 456 accepted turns (461 raw in all formats, 457 after dedup), included in `PRICE_MISSING` |
| PO1 | `sp_research_std` v2 (to 2026-02-01) overlaps v3 (from 2026-01-25) | pairwise interval-overlap check per profile | 1 overlap | resolve with evidence (PO1b) | — |
| PO1b | which version applies inside the overlap | the audit log's own `policy_version` for research sessions in the overlap window (UTC) | 346 decisions | all 346 say `3`, so **the newer version wins** | v2's effective window ends 2026-01-25 (`WINDOW_CUT_BY_NEWER_VERSION`; declared end kept) |
| PO2 | `sp_ops_elevated` v4 has no `network` block | key-presence check on the raw JSON | 1 version | no network block = no egress allowed; every egress under v4 is a violation | `NO_NETWORK_BLOCK_NO_EGRESS`; 7,423 records carry `no_egress_allowed` |
| PO3 | egress allowlists contain wildcards (`*.public-data.example`) | regex on the exploded allowlists | 7 entries | match as patterns (exactly one DNS label, case-insensitive), never `==` | `policy.host_allowed` |

## 2. `agent_activity.parquet` (`activity.py`, `timeutil.py`)

| ID | Problem | How detected | Found | Decision | Curated |
|---|---|---|---|---|---|
| AC1 | `event_time` stored as strings in several formats: ISO with ms, ISO without ms, epoch ms, `dd/mm/yyyy HH:MM:SS`, garbage | value shapes (`digits → 9`) | 15,134 non-ISO rows | parse every valid format to UTC; flag the non-ISO ones | 14,240 accepted rows flagged `NONSTANDARD_TIMESTAMP_FORMAT` |
| AC2 | epoch and `dd/mm/yyyy` values: which unit, which zone, which order? | lateness vs `ingest_time` is about 1 s for every format (so UTC, no hour offset); 13-digit values fall in the pack window only as ms; a day field > 12 proves day-first | 14,421 | UTC, epoch = ms, day-first | as AC1 |
| AC3 | unparseable or impossible `event_time` (`not-a-time`, `2026-02-30T10:00:00Z`) | format classification + strict parse | 713 | reject `MALFORMED_TIMESTAMP` | 708 rejected (5 had an earlier reason) |
| AC4 | late events (`ingest − event > 10 s`), up to 45 min on accepted rows | lateness distribution | 21,256 raw rows | accept + flag `LATE` (batch has no watermark; the 60 s Kafka watermark is M2) | 13,865 accepted rows flagged. 7,329 of the late raw rows are duplicate re-sends rejected by AC5 |
| AC4b | tool call `finished_at` earlier than its start (`event_time`) | `finished_at − event_time < 0` | 1,618 | keep; flag (no M1 metric uses `finished_at`) | 1,603 flagged `FINISHED_BEFORE_START` |
| AC5 | duplicate `event_id`: identical payload, later `ingest_time` (at-least-once delivery) | `event_id.duplicated()` | 7,427 | keep the earliest `ingest_time`; reject the others `DUPLICATE_EVENT_ID` | 7,427 rejected; `reason_detail` names the kept line |
| AC6 | unknown agents `ag_999` / `ag_321` (version `9.9.9`, often without task or model) | FK check against `agents.csv` | 1,332 | reject `UNKNOWN_AGENT_ID` | 1,332 rejected |
| AC7 | unknown tenant `tn_777` sprinkled into otherwise valid sessions | FK check against `tenants.csv` | 736 | reject `UNKNOWN_TENANT_ID` | 730 rejected |
| AC8 | `release_version` with a `v` prefix (`v2.3.1`, `v2.2.0`) | FK check fails raw and passes after stripping `v` | 2,351 | normalise + flag | 2,311 flagged `VERSION_PREFIX_STRIPPED` |
| AC9 | tool call `tool_id` holds a tool *name* (`http_get`, `webfetch`) | FK check against `tool_id`, then match against `tool_name` | 980 | resolve via the alias map (TL2) to `tl_web_fetch` + flag | 972 flagged `TOOL_ALIAS_RESOLVED` |
| AC10 | event types outside the enum (`agent.telepathy`, `llm.thought`; no sequence number) | enum check on `event_type` | 302 | reject `INVALID_EVENT_TYPE` | 302 rejected |
| AC11 | `status` null on `llm.turn` / `tool.call` | required-field null check | 738 | reject `MISSING_REQUIRED_FIELD` | 730 rejected |
| AC12 | tool call `status = 'succeeded'`, not in the enum | crosstab `event_type × status` | 160 | map to `success` + flag | 160 flagged `STATUS_NORMALISED` |
| AC12b | tool call `status = success` while `error_class` is filled | crosstab status × error_class (found during cleaning) | 17 | keep as received; flag | 17 flagged `SUCCESS_WITH_ERROR_CLASS` |
| AC13 | negative `latency_ms`, negative `input_tokens`, `retry_number = −1` | min / negative count per numeric column | 1,562 | reject `INVALID_NUMERIC_RANGE` | 1,548 rejected |
| AC14 | `llm.turn` with null `output_tokens` | null check per event type | 4,449 | keep the turn; missing-token policy = **null-excluded** (never 0) | 4,098 flagged `OUTPUT_TOKENS_MISSING`; zero-imputation would change total cost by +0.79% (`output/metrics/missing_token_effect.csv`) |
| AC15 | sessions with no `session.start` (single orphan rows) | count of `session.start` per session | 200 | reject `ORPHAN_EVENT`, judged against **accepted** starts after every other check | 200 rejected; no rejected start orphaned a session in this pack |
| AC16 | sessions without a terminal event; sessions with sequence gaps | terminal count, and `max(sequence_number)` vs row count per session | 2,248 / 700 | not a reject. Excluded from metrics 1/2 (no outcome, no duration); gaps counted by trace completeness | 2,048 accepted sessions without a terminal row; 4,035 terminal sessions with a gap in accepted sequence numbers (I6) |
| AC17 | terminal `latency_ms` disagrees with the derived duration (terminal − start) | `abs(latency_ms/1000 − duration) > 1 s` | 1,942 | the duration is always derived; `latency_ms` is only a cross-check, reported | 1,942 of 97,952 terminal sessions (1.98%), by up to ±30 s |

## 3. `network_flows.csv` (`flows.py`)

| ID | Problem | How detected | Found | Decision | Curated |
|---|---|---|---|---|---|
| NF1 | flows whose `session_id` is not a known session (`ss_700000`, sandbox `sbx_7777`) | FK check against activity sessions | 84 | reject `ORPHAN_EVENT` (against accepted starts) | 84 rejected |
| NF2 | `dst_host` empty (mostly ssh/smb/rdp to internal IPs, some https) | null check by protocol | 3,514 | fill from an unambiguous `dst_ip → host` map built from other flows; else keep the IP as target | 1,190 `DST_HOST_FILLED`, 2,317 `DST_HOST_UNKNOWN` (an unknown host can't be allowlisted, so it counts as a violation; token `egress_host_unknown`) |
| NF3 | `geo_country` empty | null check | 2,088 | keep null (documented nullable; internal destinations) | unchanged |
| NF4 | is `flow_time` UTC? | median `flow_time − tool.call start` = +0.34 s, no hour offset | 37,602 pairs | genuine UTC; flows become the clock anchor for the audit log | used as-is |

## 4. `sandbox_audit.log` (`audit.py`, `timeutil.py`)

| ID | Problem | How detected | Found | Decision | Curated |
|---|---|---|---|---|---|
| AU1 | truncated lines that do not match the syslog layout | anchored regex; lengths of the failing lines | 524 | reject `UNPARSEABLE_RECORD` with the raw line and line number | 524 rejected |
| AU2 | `policy_version` is `-` (null marker) | count of `-` per field | 2,446 | null; the applied version comes from the window join | 2,446 flagged `POLICY_VERSION_NOT_LOGGED`, all of them ag_019 lines (no policy in effect) |
| AU3 | exact duplicate lines | `raw_line.duplicated()` | 4 | keep the first, reject repeats `DUPLICATE_EVENT_ID` | 4 rejected (`identical to line N`) |
| AU4 | `log_time` is host-local with **no year and no offset**; hostnames suggest eu/us/me regions | `socket_connect` paired with UTC flows: per-host median offset 1.97 to 2.03 h, no DST jump; tool-call pairing covers all 42 hosts and agrees with flows within 0.4 s; the `Apr` lines are the +2 h spill-over of 31 March | 55,465 pairs | every host is **UTC+2** regardless of its name, plus a real per-host skew. `utc = local(2026) − measured per-host offset`; fallback exactly UTC+2 | offsets in `audit_host_offsets`: skew −90.3 to +87.4 s around UTC+2; audit vs flow after correction p01 to p99 = −0.68 to +0.39 s |

## 5. `siem_alerts.json` (`siem.py`, `correlate.py`)

| ID | Problem | How detected | Found | Decision | Curated |
|---|---|---|---|---|---|
| SI1 | `detected_at` uses a different offset per `source_system` (`+02:00`, `+03:00`, `−05:00`) | crosstab offset × system; after conversion, alerts trail flows by 0 to 45 s for every system, so the offsets are honest | 3,754 | convert with the stated offset to UTC; keep raw string and offset | `detected_at_utc`, `utc_offset` |
| SI2 | cloud-ids `*_external` alerts come from source IPs that never appear in flows | `source_ip ∈ flows.src_ip`, by rule | 1,251 | leave unmatched (off-platform activity); never forced | 1,251 of the 1,318 unmatched |
| SI3 | alerts have no `session_id`, so correlation needs a key rule and a tolerance N | N-sensitivity table: the counts are flat from 45 s to 120 s | 3,754 | rule `src_ip = source_ip ∧ dst_ip = destination.host ∧ port ∧ \|Δt\| ≤ 60 s` | **2,409 matched / 27 ambiguous / 1,318 unmatched**; 2,435 alerts tied to a session (26 ambiguous alerts whose flows share one session) |
| SI4 | nested `destination` and `technique` objects | JSON structure | — | `json_normalize` into flat columns | `destination_host/port`, `technique_id/name` |

## 6. Integration findings (found while joining the sources)

These only show up once sources are combined. They are not errors in a single file, but they change how the data must
be read.

| ID | Finding | How detected | Count | Decision |
|---|---|---|---|---|
| I1 | **The sandbox fails open.** It said `ALLOW` with reason `NO_NETWORK_POLICY` (3,705) or `WILDCARD_MATCH_UNVERIFIED` (425) to connections the effective policy forbids | sandbox decision × our window-joined policy verdict (`policy.cross_check`) | 4,130 lines | violations are judged by the **policy**, not by the sandbox's decision: these count as violations with token `allowed_despite_policy`. Audit violations = 6,463 DENY + 4,130 fail-open = 10,593 |
| I2 | the same fail-open on the network side: flows the platform let through (`verdict = allowed`) although they ran under `sp_ops_elevated` v4, which has no network block | flow verdict × `is_policy_violation` × `violation_rule` (all 3,718 are `no_egress_allowed`) | 3,718 flows | counted as violations; the flow verdict is kept as written. Flow violations total 7,458 |
| I3 | **`tl_net_probe` calls are never denied by the platform** although no policy allows the tool (811 failure, 1,263 timeout, 2 success) | tool call status × our violation | 2,076 calls | violation = tool not in `allowed_tools`, whatever the status. Every platform-denied call (4,503) is also a violation in our rule; tool-call violations total 6,583 |
| I4 | the audit log's reported `policy_version` vs our validity-window join | equality check per line | 100% equal where reported | confirms the window join and the overlap rule (PO1b) |
| I5 | duplicate deliveries are late re-sends | lateness of the rejected duplicate copies | 7,329 of 7,427 (98.7%) late | explains why the late-event rate is 1.499% on accepted rows vs about 2.27% on the raw file |
| I6 | our own rejects leave holes in sessions' sequence numbers | missing sequence numbers matched against rejected rows' `(session_id, sequence_number)` | 3,540 terminal sessions (+22 mixed); 472 have a step that was never sent | the Appendix A rule counts gaps in **accepted** sequence numbers, so these sessions are incomplete: trace completeness 95.881% |

## 7. Reject summary

| Source | Received | Accepted | Rejected | Reasons |
|---|---|---|---|---|
| `agent_activity.parquet` | 937,752 | 924,775 | 12,977 (1.384%) | DUPLICATE_EVENT_ID 7,427 · UNKNOWN_AGENT_ID 1,332 · UNKNOWN_TENANT_ID 730 · MALFORMED_TIMESTAMP 708 · ORPHAN_EVENT 200 · INVALID_EVENT_TYPE 302 · MISSING_REQUIRED_FIELD 730 · INVALID_NUMERIC_RANGE 1,548 |
| `network_flows.csv` | 42,392 | 42,308 | 84 (0.198%) | ORPHAN_EVENT 84 |
| `sandbox_audit.log` | 184,005 | 183,477 | 528 (0.287%) | DUPLICATE_EVENT_ID 4 · UNPARSEABLE_RECORD 524 |
| `siem_alerts.json` | 3,754 | 3,754 | 0 | — |
| **total** | **1,167,903** | **1,154,314** | **13,589 (1.164%)** | |
