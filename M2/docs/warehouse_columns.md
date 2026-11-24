# Warehouse columns (schema `kestrel_dw`)

Generated from `kestrel_m2/star.py` by `python -m kestrel_m2.warehouse --print-ddl`. The meaning of the columns and the policies behind them are in [`data_dictionary.md`](data_dictionary.md). Every table also has `loaded_at` (set when the row was inserted or last changed by the upsert).

## `dim_source_file`

Lab 6 source-file catalogue: one row per file content (file@sha256)

Primary key (natural key of the upsert): `(source_file_id)`

| column | type | null | key |
|---|---|---|---|
| `source_file_id` | text |  | PK |
| `file_name` | text |  |  |
| `path` | text |  |  |
| `format` | text |  |  |
| `format_detail` | text |  |  |
| `grain` | text |  |  |
| `size_bytes` | bigint |  |  |
| `content_sha256` | char(64) |  |  |
| `row_count` | bigint |  |  |
| `used_in` | text |  |  |
| `ingested_at` | timestamptz |  |  |

## `dim_date`

calendar built from the data's own timestamps (UTC days)

Primary key (natural key of the upsert): `(date_key)`

| column | type | null | key |
|---|---|---|---|
| `date_key` | integer |  | PK |
| `full_date` | date |  |  |
| `year` | smallint |  |  |
| `quarter` | smallint |  |  |
| `month` | smallint |  |  |
| `month_name` | text |  |  |
| `day` | smallint |  |  |
| `day_of_week` | smallint |  |  |
| `day_name` | text |  |  |
| `iso_year` | smallint |  |  |
| `iso_week` | smallint |  |  |
| `is_weekend` | boolean |  |  |

## `dim_time`

minute of the UTC day (0..1439) with 6-hour bands

Primary key (natural key of the upsert): `(time_key)`

| column | type | null | key |
|---|---|---|---|
| `time_key` | smallint |  | PK |
| `hour` | smallint |  |  |
| `minute` | smallint |  |  |
| `hhmm` | char(5) |  |  |
| `hour_band` | text |  |  |
| `is_business_hours` | boolean |  |  |

## `dim_tenant`

canonical tenants (tn_004 merged into tn_003)

Primary key (natural key of the upsert): `(tenant_id)`

| column | type | null | key |
|---|---|---|---|
| `tenant_id` | text |  | PK |
| `tenant_name` | text |  |  |
| `industry` | text |  |  |
| `plan_tier` | text |  |  |
| `region` | text | yes |  |
| `region_raw` | text | yes |  |
| `onboarded_at_utc` | timestamptz |  |  |
| `is_active` | boolean |  |  |
| `security_contact` | text | yes |  |
| `merged_tenant_ids` | text[] |  |  |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `dim_agent`

one row per agent version (Type-2 window kept as columns)

Primary key (natural key of the upsert): `(agent_id, release_version)`

| column | type | null | key |
|---|---|---|---|
| `agent_id` | text |  | PK |
| `release_version` | text |  | PK |
| `agent_sk` | integer |  |  |
| `release_version_raw` | text |  |  |
| `agent_name` | text |  |  |
| `agent_type` | text |  |  |
| `model_id` | text |  |  |
| `tenant_id` | text |  | FK -> `dim_tenant.tenant_id` |
| `sandbox_profile_id` | text |  |  |
| `owner_team` | text |  |  |
| `autonomy_level` | text |  |  |
| `max_tool_calls_per_session` | integer |  |  |
| `launch_date` | date |  |  |
| `is_deprecated` | boolean |  |  |
| `valid_from_utc` | timestamptz |  |  |
| `valid_to_utc` | timestamptz | yes |  |
| `is_current` | boolean |  |  |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `dim_tool`



Primary key (natural key of the upsert): `(tool_id, tool_version)`

| column | type | null | key |
|---|---|---|---|
| `tool_id` | text |  | PK |
| `tool_version` | text |  | PK |
| `tool_name` | text |  |  |
| `tool_category` | text |  |  |
| `risk_tier` | text |  |  |
| `requires_approval` | boolean |  |  |
| `owner_team` | text |  |  |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `dim_policy`

one row per policy version, flattened in DuckDB; validity dates are ordinary columns

Primary key (natural key of the upsert): `(sandbox_profile_id, policy_version)`

| column | type | null | key |
|---|---|---|---|
| `sandbox_profile_id` | text |  | PK |
| `policy_version` | integer |  | PK |
| `valid_from_utc` | timestamptz |  |  |
| `valid_to_utc` | timestamptz | yes |  |
| `valid_to_declared_utc` | timestamptz | yes |  |
| `allowed_tools` | text[] |  |  |
| `has_network_block` | boolean |  |  |
| `egress_allowlist` | text[] |  |  |
| `denied_ports` | integer[] |  |  |
| `max_egress_mb` | double precision | yes |  |
| `fs_read_scope` | text[] |  |  |
| `fs_write_scope` | text[] |  |  |
| `max_runtime_s` | integer |  |  |
| `max_memory_mb` | integer |  |  |
| `escalation_allowed` | boolean |  |  |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `policy_allowed_tool`



Primary key (natural key of the upsert): `(sandbox_profile_id, policy_version, tool_id)`

| column | type | null | key |
|---|---|---|---|
| `sandbox_profile_id` | text |  | PK FK -> `dim_policy.sandbox_profile_id` |
| `policy_version` | integer |  | PK FK -> `dim_policy.policy_version` |
| `tool_id` | text |  | PK |

## `policy_egress_rule`



Primary key (natural key of the upsert): `(sandbox_profile_id, policy_version, allowlist_entry)`

| column | type | null | key |
|---|---|---|---|
| `sandbox_profile_id` | text |  | PK FK -> `dim_policy.sandbox_profile_id` |
| `policy_version` | integer |  | PK FK -> `dim_policy.policy_version` |
| `allowlist_entry` | text |  | PK |
| `is_wildcard` | boolean |  |  |
| `match_regex` | text |  |  |

## `dim_model_price`



Primary key (natural key of the upsert): `(model_id, effective_from_utc)`

| column | type | null | key |
|---|---|---|---|
| `model_id` | text |  | PK |
| `effective_from_utc` | timestamptz |  | PK |
| `effective_to_utc` | timestamptz | yes |  |
| `model_family` | text |  |  |
| `input_price_per_1k` | numeric(12,6) | yes |  |
| `output_price_per_1k` | numeric(12,6) | yes |  |
| `currency` | text |  |  |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `fact_session`

one row per accepted session (accumulating snapshot of the whole session)

Primary key (natural key of the upsert): `(session_id)`

| column | type | null | key |
|---|---|---|---|
| `session_id` | text |  | PK |
| `tenant_id` | text |  | FK -> `dim_tenant.tenant_id` |
| `agent_id` | text |  | FK -> `dim_agent.agent_id` |
| `release_version` | text |  | FK -> `dim_agent.release_version` |
| `agent_sk` | integer |  |  |
| `sandbox_profile_id` | text | yes | FK -> `dim_policy.sandbox_profile_id` |
| `policy_version_applied` | integer | yes | FK -> `dim_policy.policy_version` |
| `sandbox_instance_id` | text |  |  |
| `task_id` | text |  |  |
| `task_category` | text |  |  |
| `expected_tool_categories` | text |  |  |
| `start_time_utc` | timestamptz |  |  |
| `date_key` | integer |  | FK -> `dim_date.date_key` |
| `time_key` | smallint |  | FK -> `dim_time.time_key` |
| `end_time_utc` | timestamptz | yes |  |
| `terminal_event_type` | text | yes |  |
| `outcome` | text | yes |  |
| `duration_s` | double precision | yes |  |
| `terminal_latency_ms` | bigint | yes |  |
| `latency_disagrees` | boolean | yes |  |
| `n_events` | integer |  |  |
| `n_prompt_turns` | integer |  |  |
| `n_tool_calls` | integer |  |  |
| `n_tool_violations` | integer |  |  |
| `n_tool_denied` | integer |  |  |
| `n_tool_failed` | integer |  |  |
| `n_network_flows` | integer |  |  |
| `n_egress_violations` | integer |  |  |
| `n_distinct_destinations` | integer |  |  |
| `n_sandbox_decisions` | integer |  |  |
| `n_sandbox_denials` | integer |  |  |
| `n_escalation_attempts` | integer |  |  |
| `n_escalation_denied` | integer |  |  |
| `n_injection_turns` | integer |  |  |
| `n_late_events` | integer |  |  |
| `has_sequence_gap` | boolean |  |  |
| `is_trace_complete` | boolean |  |  |
| `executed_tool_categories` | text |  |  |
| `is_goal_drift` | boolean | yes |  |
| `first_injection_time_utc` | timestamptz | yes |  |
| `first_violation_time_utc` | timestamptz | yes |  |
| `first_violation_after_injection_utc` | timestamptz | yes |  |
| `tokens_in` | bigint | yes |  |
| `tokens_out` | bigint | yes |  |
| `cost_usd` | numeric(14,6) | yes |  |
| `start_activity_id` | text |  |  |
| `terminal_activity_id` | text | yes |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `fact_prompt_turn`



Primary key (natural key of the upsert): `(event_id)`

| column | type | null | key |
|---|---|---|---|
| `event_id` | text |  | PK |
| `activity_id` | text |  |  |
| `session_id` | text |  | FK -> `fact_session.session_id` |
| `tenant_id` | text |  | FK -> `dim_tenant.tenant_id` |
| `agent_id` | text |  | FK -> `dim_agent.agent_id` |
| `release_version` | text |  | FK -> `dim_agent.release_version` |
| `agent_sk` | integer |  |  |
| `sandbox_profile_id` | text | yes | FK -> `dim_policy.sandbox_profile_id` |
| `policy_version_applied` | integer | yes | FK -> `dim_policy.policy_version` |
| `event_time_utc` | timestamptz |  |  |
| `ingest_time_utc` | timestamptz |  |  |
| `lateness_s` | double precision |  |  |
| `is_late` | boolean |  |  |
| `date_key` | integer |  | FK -> `dim_date.date_key` |
| `time_key` | smallint |  | FK -> `dim_time.time_key` |
| `sequence_number` | integer |  |  |
| `turn_index` | integer |  |  |
| `status` | text |  |  |
| `latency_ms` | bigint |  |  |
| `model_id` | text |  |  |
| `content_source` | text |  |  |
| `stated_goal` | text |  |  |
| `injection_indicators` | text |  |  |
| `n_injection_indicators` | smallint |  |  |
| `finish_reason` | text |  |  |
| `tokens_in` | bigint |  |  |
| `tokens_out` | bigint | yes |  |
| `input_price_per_1k` | numeric(12,6) | yes |  |
| `output_price_per_1k` | numeric(12,6) | yes |  |
| `cost_usd` | numeric(14,6) | yes |  |
| `event_time_format` | text |  |  |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `fact_tool_call`



Primary key (natural key of the upsert): `(event_id)`

| column | type | null | key |
|---|---|---|---|
| `event_id` | text |  | PK |
| `activity_id` | text |  |  |
| `session_id` | text |  | FK -> `fact_session.session_id` |
| `tenant_id` | text |  | FK -> `dim_tenant.tenant_id` |
| `agent_id` | text |  | FK -> `dim_agent.agent_id` |
| `release_version` | text |  | FK -> `dim_agent.release_version` |
| `agent_sk` | integer |  |  |
| `sandbox_profile_id` | text | yes | FK -> `dim_policy.sandbox_profile_id` |
| `policy_version_applied` | integer | yes | FK -> `dim_policy.policy_version` |
| `event_time_utc` | timestamptz |  |  |
| `ingest_time_utc` | timestamptz |  |  |
| `lateness_s` | double precision |  |  |
| `is_late` | boolean |  |  |
| `date_key` | integer |  | FK -> `dim_date.date_key` |
| `time_key` | smallint |  | FK -> `dim_time.time_key` |
| `sequence_number` | integer |  |  |
| `tool_id` | text |  | FK -> `dim_tool.tool_id` |
| `tool_version` | text |  | FK -> `dim_tool.tool_version` |
| `tool_id_raw` | text |  |  |
| `status` | text |  |  |
| `status_raw` | text |  |  |
| `error_class` | text | yes |  |
| `retry_number` | integer |  |  |
| `latency_ms` | bigint |  |  |
| `finished_at_utc` | timestamptz |  |  |
| `target_resource` | text |  |  |
| `bytes_out` | bigint | yes |  |
| `is_policy_violation` | boolean | yes |  |
| `risk_indicators` | text |  |  |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `fact_network_flow`



Primary key (natural key of the upsert): `(flow_id)`

| column | type | null | key |
|---|---|---|---|
| `flow_id` | text |  | PK |
| `activity_id` | text |  |  |
| `session_id` | text |  | FK -> `fact_session.session_id` |
| `tenant_id` | text |  | FK -> `dim_tenant.tenant_id` |
| `agent_id` | text |  | FK -> `dim_agent.agent_id` |
| `release_version` | text |  | FK -> `dim_agent.release_version` |
| `agent_sk` | integer |  |  |
| `sandbox_profile_id` | text | yes | FK -> `dim_policy.sandbox_profile_id` |
| `policy_version_applied` | integer | yes | FK -> `dim_policy.policy_version` |
| `flow_time_utc` | timestamptz |  |  |
| `date_key` | integer |  | FK -> `dim_date.date_key` |
| `time_key` | smallint |  | FK -> `dim_time.time_key` |
| `sandbox_instance_id` | text |  |  |
| `src_ip` | text |  |  |
| `dst_host` | text | yes |  |
| `dst_host_raw` | text | yes |  |
| `dst_ip` | text |  |  |
| `dst_port` | integer |  |  |
| `protocol` | text |  |  |
| `bytes_sent` | bigint |  |  |
| `bytes_received` | bigint |  |  |
| `verdict` | text |  |  |
| `geo_country` | text | yes |  |
| `is_policy_violation` | boolean | yes |  |
| `violation_rule` | text |  |  |
| `risk_indicators` | text |  |  |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `fact_sandbox_decision`



Primary key (natural key of the upsert): `(audit_id)`

| column | type | null | key |
|---|---|---|---|
| `audit_id` | text |  | PK |
| `activity_id` | text |  |  |
| `session_id` | text |  | FK -> `fact_session.session_id` |
| `tenant_id` | text |  | FK -> `dim_tenant.tenant_id` |
| `agent_id` | text |  | FK -> `dim_agent.agent_id` |
| `release_version` | text |  | FK -> `dim_agent.release_version` |
| `agent_sk` | integer |  |  |
| `sandbox_profile_id` | text | yes | FK -> `dim_policy.sandbox_profile_id` |
| `policy_version_applied` | integer | yes | FK -> `dim_policy.policy_version` |
| `event_time_utc` | timestamptz |  |  |
| `date_key` | integer |  | FK -> `dim_date.date_key` |
| `time_key` | smallint |  | FK -> `dim_time.time_key` |
| `log_time_raw` | text |  |  |
| `host` | text |  |  |
| `host_offset_s` | double precision |  |  |
| `process` | text |  |  |
| `pid` | integer |  |  |
| `sandbox_instance_id` | text |  |  |
| `decision` | text |  |  |
| `action` | text |  |  |
| `is_escalation_action` | boolean |  |  |
| `target` | text |  |  |
| `reason` | text |  |  |
| `policy_version_reported` | integer | yes |  |
| `is_policy_violation` | boolean | yes |  |
| `violation_rule` | text |  |  |
| `risk_indicators` | text |  |  |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `fact_siem_alert`

severity is the targeted system's opinion, carried as received

Primary key (natural key of the upsert): `(alert_id)`

| column | type | null | key |
|---|---|---|---|
| `alert_id` | text |  | PK |
| `activity_id` | text |  |  |
| `detected_at_utc` | timestamptz |  |  |
| `date_key` | integer |  | FK -> `dim_date.date_key` |
| `time_key` | smallint |  | FK -> `dim_time.time_key` |
| `detected_at_raw` | text |  |  |
| `utc_offset` | text |  |  |
| `source_system` | text |  |  |
| `severity` | text |  |  |
| `source_ip` | text |  |  |
| `destination_host` | text |  |  |
| `destination_port` | integer |  |  |
| `technique_id` | text |  |  |
| `technique_name` | text | yes |  |
| `rule` | text |  |  |
| `confidence` | double precision |  |  |
| `match_status` | text |  |  |
| `n_candidate_flows` | integer |  |  |
| `n_candidate_sessions` | integer |  |  |
| `matched_flow_id` | text | yes | FK -> `fact_network_flow.flow_id` |
| `session_id` | text | yes | FK -> `fact_session.session_id` |
| `session_resolution` | text |  |  |
| `delta_s` | double precision | yes |  |
| `tenant_id` | text | yes | FK -> `dim_tenant.tenant_id` |
| `agent_id` | text | yes | FK -> `dim_agent.agent_id` |
| `release_version` | text | yes | FK -> `dim_agent.release_version` |
| `quality_flags` | text |  |  |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_line` | bigint |  |  |

## `fact_rejected_record`

every refused record (batch and stream), exactly one enum reason, pointer to file and line

Primary key (natural key of the upsert): `(source_file, source_line)`

| column | type | null | key |
|---|---|---|---|
| `source_file` | text |  | PK |
| `source_line` | bigint |  | PK |
| `source_file_id` | text |  | FK -> `dim_source_file.source_file_id` |
| `source_format` | text |  |  |
| `record_id` | text | yes |  |
| `reason` | text |  |  |
| `reason_detail` | text | yes |  |
| `raw_payload` | text |  |  |
| `zone` | text |  |  |
| `channel` | text |  |  |
| `detected_by` | text |  |  |
| `kafka_topic` | text | yes |  |
| `kafka_partition` | integer | yes |  |
| `kafka_offset` | bigint | yes |  |
