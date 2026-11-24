-- Refresh of the four aggregate tables: delete-then-insert of the whole table (one
-- partition), inside the caller's transaction. Pure SQL over the facts.

-- ---------------------------------------------------------------- agent_daily_behaviour
-- grain: (UTC day, agent version). Sessions count on their start day, events on their own day.
DELETE FROM {s}.agent_daily_behaviour;
WITH s AS (
    SELECT date_key, agent_id, release_version, count(*) AS sessions_started,
           count(outcome) AS sessions_terminal,
           count(*) FILTER (WHERE outcome = 'success') AS sessions_success,
           count(*) FILTER (WHERE outcome = 'error') AS sessions_error
    FROM {s}.fact_session GROUP BY 1, 2, 3
), p AS (
    SELECT date_key, agent_id, release_version, count(*) AS prompt_turns,
           count(*) FILTER (WHERE n_injection_indicators > 0) AS injection_turns,
           sum(tokens_in) AS tokens_in, coalesce(sum(tokens_out), 0) AS tokens_out,
           coalesce(sum(cost_usd), 0) AS cost_usd
    FROM {s}.fact_prompt_turn GROUP BY 1, 2, 3
), tc AS (
    SELECT date_key, agent_id, release_version, count(*) AS tool_calls,
           count(*) FILTER (WHERE is_policy_violation) AS tool_violations,
           count(*) FILTER (WHERE is_policy_violation IS NOT NULL) AS tool_judged,
           count(*) FILTER (WHERE status = 'denied') AS tool_denied,
           count(*) FILTER (WHERE status IN ('failure', 'timeout')) AS tool_failed,
           count(*) FILTER (WHERE retry_number > 0) AS tool_retries
    FROM {s}.fact_tool_call GROUP BY 1, 2, 3
), f AS (
    SELECT date_key, agent_id, release_version, count(*) AS network_flows,
           count(*) FILTER (WHERE is_policy_violation) AS egress_violations,
           count(*) FILTER (WHERE is_policy_violation IS NOT NULL) AS flows_judged
    FROM {s}.fact_network_flow GROUP BY 1, 2, 3
), d AS (
    SELECT date_key, agent_id, release_version, count(*) AS sandbox_decisions,
           count(*) FILTER (WHERE decision = 'DENY') AS sandbox_denials,
           count(*) FILTER (WHERE is_escalation_action) AS escalation_attempts
    FROM {s}.fact_sandbox_decision GROUP BY 1, 2, 3
), k AS (
    SELECT date_key, agent_id, release_version FROM s UNION SELECT date_key, agent_id, release_version FROM p
    UNION SELECT date_key, agent_id, release_version FROM tc UNION SELECT date_key, agent_id, release_version FROM f
    UNION SELECT date_key, agent_id, release_version FROM d
)
INSERT INTO {s}.agent_daily_behaviour
SELECT k.date_key, k.agent_id, k.release_version,
       coalesce(s.sessions_started, 0), coalesce(s.sessions_terminal, 0), coalesce(s.sessions_success, 0),
       coalesce(s.sessions_error, 0), coalesce(p.prompt_turns, 0), coalesce(p.injection_turns, 0),
       coalesce(tc.tool_calls, 0), coalesce(tc.tool_violations, 0), coalesce(tc.tool_denied, 0),
       coalesce(tc.tool_failed, 0), coalesce(tc.tool_retries, 0), coalesce(f.network_flows, 0),
       coalesce(f.egress_violations, 0), coalesce(d.sandbox_decisions, 0), coalesce(d.sandbox_denials, 0),
       coalesce(d.escalation_attempts, 0), coalesce(p.tokens_in, 0), coalesce(p.tokens_out, 0),
       coalesce(p.cost_usd, 0),
       round(s.sessions_error::numeric / nullif(s.sessions_terminal, 0), 6),
       round(tc.tool_violations::numeric / nullif(tc.tool_judged, 0), 6),
       round(f.egress_violations::numeric / nullif(f.flows_judged, 0), 6),
       round(p.injection_turns::numeric / nullif(p.prompt_turns, 0), 6)
FROM k
LEFT JOIN s USING (date_key, agent_id, release_version)
LEFT JOIN p USING (date_key, agent_id, release_version)
LEFT JOIN tc USING (date_key, agent_id, release_version)
LEFT JOIN f USING (date_key, agent_id, release_version)
LEFT JOIN d USING (date_key, agent_id, release_version);

-- ------------------------------------------------------------------ tenant_risk_metrics
-- grain: tenant, whole batch window. Session-level counts come from fact_session.
DELETE FROM {s}.tenant_risk_metrics;
WITH s AS (
    SELECT tenant_id, count(*) AS sessions, count(outcome) AS terminal_sessions,
           count(*) FILTER (WHERE outcome = 'error') AS error_sessions,
           count(*) FILTER (WHERE n_escalation_denied > 0) AS escalation_denied_sessions,
           count(*) FILTER (WHERE is_goal_drift) AS goal_drift_sessions,
           count(is_goal_drift) AS goal_drift_population,
           count(*) FILTER (WHERE first_violation_after_injection_utc IS NOT NULL) AS injection_then_violation_sessions
    FROM {s}.fact_session GROUP BY 1
), p AS (
    SELECT tenant_id, count(*) AS prompt_turns, count(*) FILTER (WHERE n_injection_indicators > 0) AS injection_turns,
           sum(tokens_in) AS tokens_in, coalesce(sum(tokens_out), 0) AS tokens_out, coalesce(sum(cost_usd), 0) AS cost_usd
    FROM {s}.fact_prompt_turn GROUP BY 1
), tc AS (
    SELECT tenant_id, count(*) AS tool_calls, count(*) FILTER (WHERE is_policy_violation) AS tool_violations,
           count(is_policy_violation) AS tool_judged
    FROM {s}.fact_tool_call GROUP BY 1
), f AS (
    SELECT tenant_id, count(*) AS network_flows, count(*) FILTER (WHERE is_policy_violation) AS egress_violations,
           count(is_policy_violation) AS flows_judged
    FROM {s}.fact_network_flow GROUP BY 1
), d AS (
    SELECT tenant_id, count(*) AS sandbox_decisions, count(*) FILTER (WHERE decision = 'DENY') AS sandbox_denials
    FROM {s}.fact_sandbox_decision GROUP BY 1
), a AS (
    SELECT tenant_id, count(*) AS siem_alerts_linked,
           count(*) FILTER (WHERE severity IN ('high', 'critical')) AS siem_alerts_high_critical
    FROM {s}.fact_siem_alert WHERE tenant_id IS NOT NULL GROUP BY 1
)
INSERT INTO {s}.tenant_risk_metrics
SELECT t.tenant_id, coalesce(s.sessions, 0), coalesce(s.terminal_sessions, 0),
       round(s.error_sessions::numeric / nullif(s.terminal_sessions, 0), 6),
       coalesce(tc.tool_calls, 0), coalesce(tc.tool_violations, 0),
       round(tc.tool_violations::numeric / nullif(tc.tool_judged, 0), 6),
       coalesce(f.network_flows, 0), coalesce(f.egress_violations, 0),
       round(f.egress_violations::numeric / nullif(f.flows_judged, 0), 6),
       coalesce(d.sandbox_decisions, 0), coalesce(d.sandbox_denials, 0),
       round(d.sandbox_denials::numeric / nullif(d.sandbox_decisions, 0), 6),
       coalesce(s.escalation_denied_sessions, 0), coalesce(p.prompt_turns, 0), coalesce(p.injection_turns, 0),
       round(p.injection_turns::numeric / nullif(p.prompt_turns, 0), 6),
       coalesce(s.goal_drift_sessions, 0),
       round(s.goal_drift_sessions::numeric / nullif(s.goal_drift_population, 0), 6),
       coalesce(s.injection_then_violation_sessions, 0),
       coalesce(a.siem_alerts_linked, 0), coalesce(a.siem_alerts_high_critical, 0),
       coalesce(p.tokens_in, 0), coalesce(p.tokens_out, 0), coalesce(p.cost_usd, 0)
FROM {s}.dim_tenant t
LEFT JOIN s USING (tenant_id) LEFT JOIN p USING (tenant_id) LEFT JOIN tc USING (tenant_id)
LEFT JOIN f USING (tenant_id) LEFT JOIN d USING (tenant_id) LEFT JOIN a USING (tenant_id);

-- ------------------------------------------------------------- policy_violation_metrics
-- grain: (policy version in effect at event time, activity type). Rows without a policy
-- in effect (ag_019, sp_missing_profile) cannot be judged and are not counted here.
DELETE FROM {s}.policy_violation_metrics;
WITH v AS (
    SELECT sandbox_profile_id, policy_version_applied AS policy_version, 'tool_call' AS activity_type, session_id,
           is_policy_violation, CASE WHEN is_policy_violation THEN 'tool_not_allowed' END AS rule
    FROM {s}.fact_tool_call WHERE policy_version_applied IS NOT NULL
    UNION ALL
    SELECT sandbox_profile_id, policy_version_applied, 'network_flow', session_id, is_policy_violation,
           nullif(violation_rule, '')
    FROM {s}.fact_network_flow WHERE policy_version_applied IS NOT NULL
    UNION ALL
    SELECT sandbox_profile_id, policy_version_applied, 'sandbox_decision', session_id, is_policy_violation,
           nullif(violation_rule, '')
    FROM {s}.fact_sandbox_decision WHERE policy_version_applied IS NOT NULL
), r AS (
    SELECT sandbox_profile_id, policy_version, activity_type,
           string_agg(rule || '=' || n, '|' ORDER BY n DESC, rule) AS rule_breakdown
    FROM (SELECT sandbox_profile_id, policy_version, activity_type, rule, count(*) AS n
          FROM v WHERE rule IS NOT NULL GROUP BY 1, 2, 3, 4) x
    GROUP BY 1, 2, 3
)
INSERT INTO {s}.policy_violation_metrics
SELECT v.sandbox_profile_id, v.policy_version, v.activity_type, count(*),
       count(*) FILTER (WHERE v.is_policy_violation),
       round(count(*) FILTER (WHERE v.is_policy_violation)::numeric / count(*), 6),
       count(DISTINCT v.session_id) FILTER (WHERE v.is_policy_violation),
       coalesce(max(r.rule_breakdown), '')
FROM v LEFT JOIN r USING (sandbox_profile_id, policy_version, activity_type)
GROUP BY 1, 2, 3;

-- ----------------------------------------------------------------- data_quality_metrics
-- grain: (channel, source file, metric, code). received = the catalogue's record count
-- (batch) or the stream's message log (stream).
DELETE FROM {s}.data_quality_metrics;
WITH received AS (
    SELECT 'batch' AS channel, c.file_name AS source_file, c.row_count AS n
    FROM {s}.dim_source_file c
    WHERE c.file_name IN ('agent_activity.parquet', 'network_flows.csv', 'sandbox_audit.log', 'siem_alerts.json')
      AND c.ingested_at = (SELECT max(ingested_at) FROM {s}.dim_source_file x WHERE x.file_name = c.file_name)
    UNION ALL
    SELECT 'stream', 'stream_events.json', count(*) FROM {s}.stream_message_log
    WHERE kafka_topic = 'kestrel.agent.events' AND outcome <> 'control'
    HAVING count(*) > 0
), rejected AS (
    SELECT channel, source_file, reason, count(*) AS n FROM {s}.fact_rejected_record GROUP BY 1, 2, 3
), accepted AS (
    -- every accepted activity row belongs to exactly one session, so the session fact counts them all
    SELECT 'batch' AS channel, 'agent_activity.parquet' AS source_file, sum(n_events) AS n,
           sum(n_late_events) AS late_turns_and_calls
    FROM {s}.fact_session
    UNION ALL SELECT 'batch', 'network_flows.csv', (SELECT count(*) FROM {s}.fact_network_flow), NULL
    UNION ALL SELECT 'batch', 'sandbox_audit.log', (SELECT count(*) FROM {s}.fact_sandbox_decision), NULL
    UNION ALL SELECT 'batch', 'siem_alerts.json', (SELECT count(*) FROM {s}.fact_siem_alert), NULL
    UNION ALL SELECT 'stream', 'stream_events.json', count(*), count(*) FILTER (WHERE is_late)
    FROM {s}.stream_curated_event HAVING count(*) > 0
), flags AS (
    SELECT 'batch' AS channel, 'agent_activity.parquet' AS source_file, flag, count(*) AS n
    FROM (SELECT unnest(string_to_array(quality_flags, '|')) AS flag FROM {s}.fact_prompt_turn
          UNION ALL SELECT unnest(string_to_array(quality_flags, '|')) FROM {s}.fact_tool_call) x
    WHERE flag <> '' GROUP BY 1, 2, 3
    UNION ALL
    SELECT 'batch', 'network_flows.csv', flag, count(*)
    FROM (SELECT unnest(string_to_array(quality_flags, '|')) AS flag FROM {s}.fact_network_flow) x
    WHERE flag <> '' GROUP BY 1, 2, 3
    UNION ALL
    SELECT 'batch', 'sandbox_audit.log', flag, count(*)
    FROM (SELECT unnest(string_to_array(quality_flags, '|')) AS flag FROM {s}.fact_sandbox_decision) x
    WHERE flag <> '' GROUP BY 1, 2, 3
    UNION ALL
    SELECT 'stream', 'stream_events.json', flag, count(*)
    FROM (SELECT unnest(string_to_array(quality_flags, '|')) AS flag FROM {s}.stream_curated_event) x
    WHERE flag <> '' GROUP BY 1, 2, 3
)
INSERT INTO {s}.data_quality_metrics
SELECT r.channel, r.source_file, 'received', 'ALL', r.n, r.n, 1 FROM received r
UNION ALL
SELECT r.channel, r.source_file, 'rejected', x.reason, x.n, r.n, round(x.n::numeric / r.n, 6)
FROM received r JOIN rejected x USING (channel, source_file)
UNION ALL
SELECT r.channel, r.source_file, 'rejected', 'ALL', coalesce(sum(x.n), 0), r.n,
       round(coalesce(sum(x.n), 0)::numeric / r.n, 6)
FROM received r LEFT JOIN rejected x USING (channel, source_file) GROUP BY r.channel, r.source_file, r.n
UNION ALL
SELECT a.channel, a.source_file, 'accepted', 'ALL', a.n, r.n, round(a.n::numeric / r.n, 6)
FROM accepted a JOIN received r USING (channel, source_file)
UNION ALL
SELECT a.channel, a.source_file, 'late', 'LATE_GT_10S', a.late_turns_and_calls, a.n,
       round(a.late_turns_and_calls::numeric / nullif(a.n, 0), 6)
FROM accepted a WHERE a.late_turns_and_calls IS NOT NULL
UNION ALL
SELECT f.channel, f.source_file, 'quality_flag', f.flag, f.n, a.n, round(f.n::numeric / nullif(a.n, 0), 6)
FROM flags f JOIN accepted a USING (channel, source_file);
