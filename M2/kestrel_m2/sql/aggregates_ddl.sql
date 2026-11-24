-- Aggregate tables of the warehouse (refreshed by aggregates.sql, delete-then-insert).
-- Rates are numeric(9,6) fractions (0..1) so a refresh is bit-for-bit reproducible.

CREATE TABLE IF NOT EXISTS {s}.agent_daily_behaviour (
    date_key integer NOT NULL REFERENCES {s}.dim_date (date_key),
    agent_id text NOT NULL,
    release_version text NOT NULL,
    sessions_started integer NOT NULL,
    sessions_terminal integer NOT NULL,
    sessions_success integer NOT NULL,
    sessions_error integer NOT NULL,
    prompt_turns integer NOT NULL,
    injection_turns integer NOT NULL,
    tool_calls integer NOT NULL,
    tool_violations integer NOT NULL,
    tool_denied integer NOT NULL,
    tool_failed integer NOT NULL,
    tool_retries integer NOT NULL,
    network_flows integer NOT NULL,
    egress_violations integer NOT NULL,
    sandbox_decisions integer NOT NULL,
    sandbox_denials integer NOT NULL,
    escalation_attempts integer NOT NULL,
    tokens_in bigint NOT NULL,
    tokens_out bigint NOT NULL,
    cost_usd numeric(14,6) NOT NULL,
    session_error_rate numeric(9,6),
    tool_violation_rate numeric(9,6),
    egress_violation_rate numeric(9,6),
    injection_turn_rate numeric(9,6),
    PRIMARY KEY (date_key, agent_id, release_version),
    FOREIGN KEY (agent_id, release_version) REFERENCES {s}.dim_agent (agent_id, release_version)
);

CREATE TABLE IF NOT EXISTS {s}.tenant_risk_metrics (
    tenant_id text PRIMARY KEY REFERENCES {s}.dim_tenant (tenant_id),
    sessions integer NOT NULL,
    terminal_sessions integer NOT NULL,
    session_error_rate numeric(9,6),
    tool_calls integer NOT NULL,
    tool_violations integer NOT NULL,
    tool_violation_rate numeric(9,6),
    network_flows integer NOT NULL,
    egress_violations integer NOT NULL,
    egress_violation_rate numeric(9,6),
    sandbox_decisions integer NOT NULL,
    sandbox_denials integer NOT NULL,
    sandbox_denial_rate numeric(9,6),
    escalation_denied_sessions integer NOT NULL,
    prompt_turns integer NOT NULL,
    injection_turns integer NOT NULL,
    injection_turn_rate numeric(9,6),
    goal_drift_sessions integer NOT NULL,
    goal_drift_rate numeric(9,6),
    injection_then_violation_sessions integer NOT NULL,
    siem_alerts_linked integer NOT NULL,
    siem_alerts_high_critical integer NOT NULL,
    tokens_in bigint NOT NULL,
    tokens_out bigint NOT NULL,
    cost_usd numeric(14,6) NOT NULL
);

CREATE TABLE IF NOT EXISTS {s}.policy_violation_metrics (
    sandbox_profile_id text NOT NULL,
    policy_version integer NOT NULL,
    activity_type text NOT NULL,
    records integer NOT NULL,
    violations integer NOT NULL,
    violation_rate numeric(9,6),
    sessions_with_violation integer NOT NULL,
    rule_breakdown text NOT NULL,
    PRIMARY KEY (sandbox_profile_id, policy_version, activity_type),
    FOREIGN KEY (sandbox_profile_id, policy_version) REFERENCES {s}.dim_policy (sandbox_profile_id, policy_version)
);

CREATE TABLE IF NOT EXISTS {s}.data_quality_metrics (
    channel text NOT NULL,
    source_file text NOT NULL,
    metric text NOT NULL,
    code text NOT NULL,
    records bigint NOT NULL,
    denominator bigint NOT NULL,
    rate numeric(9,6),
    PRIMARY KEY (channel, source_file, metric, code)
);
