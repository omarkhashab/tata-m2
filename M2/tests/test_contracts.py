"""Pydantic contracts: one enum reason per refused record, repairs reported, never raises."""

import pytest

from kestrel_m2 import contracts as c

BASE = dict(event_id="ev_1", event_time="2026-03-01T10:00:00.123Z", ingest_time="2026-03-01T10:00:01Z",
            sequence_number=2, session_id="ss_1", tenant_id="tn_001", agent_id="ag_004", release_version="3.0.2",
            sandbox_instance_id="sbx_1")
TOOL = dict(BASE, event_type="tool.call", status="success", latency_ms=5, tool_id="tl_x", tool_version="1.0.0",
            finished_at="2026-03-01T10:00:02Z", retry_number=0, target_resource="host")
TURN = dict(BASE, event_type="llm.turn", status="ok", latency_ms=1, turn_index=0, model_id="m", content_source="user",
            stated_goal="g", input_tokens=10, output_tokens=None, finish_reason="stop")


@pytest.mark.parametrize("record, reason", [
    (TOOL, None),
    (dict(TOOL, event_time=1775003417121), None),                      # epoch ms as a JSON int (stream)
    (dict(TOOL, event_time="01/04/2026 00:10:08"), None),              # day-first slash date
    (TURN, None),                                                      # output_tokens may be null
    (dict(TOOL, event_time="2026-04-31T00:00:00Z"), "MALFORMED_TIMESTAMP"),
    (dict(TOOL, ingest_time="1775003417121"), "MALFORMED_TIMESTAMP"),  # ingest_time must be ISO
    (dict(TOOL, event_type="agent.telepathy"), "INVALID_EVENT_TYPE"),
    (dict(TOOL, event_type="llm.thought", event_time="nope"), "MALFORMED_TIMESTAMP"),   # earlier reason wins
    (dict(TOOL, event_type=None), "MISSING_REQUIRED_FIELD"),
    ({k: v for k, v in TOOL.items() if k != "status"}, "MISSING_REQUIRED_FIELD"),
    (dict(TOOL, retry_number=-1), "INVALID_NUMERIC_RANGE"),
    (dict(TOOL, retry_number=-1, status=None), "MISSING_REQUIRED_FIELD"),               # enum order
    (dict(TURN, input_tokens=-3), "INVALID_NUMERIC_RANGE"),
    (dict(TOOL, status="fine"), "SCHEMA_VIOLATION"),
    (dict(TOOL, event_id="x_1"), "SCHEMA_VIOLATION"),
    (dict(TOOL, latency_ms="fast"), "SCHEMA_VIOLATION"),
    ("{\"event_id\": \"ev_1\", \"event_ti", "UNPARSEABLE_RECORD"),
])
def test_activity_contract_gives_exactly_one_reason(record, reason):
    v = c.check_activity(record)
    assert v.reason == reason
    assert (v.value is None) == (reason is not None)


def test_synonym_is_repaired_and_reported():
    v = c.check_activity(dict(TOOL, status="succeeded"))
    assert v.ok and v.value.status == "success" and v.repairs == ["STATUS_NORMALISED"]


def test_reference_contracts_repair_instead_of_reject():
    t = c.check(c.Tenant, dict(tenant_id="tn_005", tenant_name="Orion", industry="retail", plan_tier="growth",
                               region="eu-central-9", onboarded_at="2025-01-01T00:00:00Z", is_active="true"))
    assert t.ok and t.value.region is None and t.repairs == ["INVALID_ENUM_REGION"]
    a = c.check(c.Agent, dict(agent_id="ag_007", release_version="v2.3.1", agent_name="x", agent_type="research",
                              model_id="m", tenant_id="tn_003", sandbox_profile_id="sp_x", owner_team="t",
                              autonomy_level="supervised", max_tool_calls_per_session="10",
                              launch_date="2026-01-01", is_deprecated="false"))
    assert a.ok and a.value.release_version == "2.3.1" and a.repairs == ["VERSION_PREFIX_STRIPPED"]


def test_policy_without_network_block_is_valid_and_nested_errors_are_mapped():
    pol = dict(sandbox_profile_id="sp_ops_elevated", policy_version=4, valid_from="2026-02-15T00:00:00Z",
               valid_to=None, allowed_tools=["tl_shell_exec"], filesystem={"read_scope": [], "write_scope": []},
               resources={"max_runtime_s": 1, "max_memory_mb": 1}, escalation_allowed=True)
    assert c.check(c.SandboxPolicy, pol).ok
    bad = dict(pol, network={"egress_allowlist": [], "denied_ports": [70000], "max_egress_mb": 1})
    assert c.check(c.SandboxPolicy, bad).reason == "INVALID_NUMERIC_RANGE"


def test_siem_and_audit_contracts():
    alert = dict(alert_id="al_1", source_system="corp-siem", severity="high", detected_at="2026-03-14T13:22:41+02:00",
                 source_ip="10.20.9.113", destination={"host": "10.20.4.19", "port": 445},
                 technique={"id": "T1021.002"}, rule="r", confidence=0.8)
    assert c.check(c.SiemAlert, alert).ok
    assert c.check(c.SiemAlert, dict(alert, detected_at="2026-03-14T13:22:41Z")).reason == "MALFORMED_TIMESTAMP"
    assert c.check(c.SiemAlert, dict(alert, confidence=1.5)).reason == "INVALID_NUMERIC_RANGE"
    line = dict(log_time="Mar 14 11:22:04", host="sbx-eu-113", process="kestrel-sandbox", pid="8841", session_id="ss_1",
                sandbox_instance_id="sbx_1", decision="DENY", action="socket_connect", target="10.0.0.1:445",
                reason="EGRESS_NOT_ALLOWED", policy_version="-")
    v = c.check(c.AuditDecision, line)
    assert v.ok and v.value.policy_version is None
    assert c.check(c.AuditDecision, dict(line, session_id="-")).reason == "MISSING_REQUIRED_FIELD"
    assert c.check(c.AuditDecision, dict(line, log_time="Feb 30 10:00:00")).reason == "MALFORMED_TIMESTAMP"


def test_eos_control_record():
    assert c.check(c.EndOfStream, {"event_type": "EOS", "event_time": "2026-04-01T00:32:11.488Z",
                                   "producer_id": "replay-1"}).ok


def test_stream_field_allowlist_has_no_free_text():
    assert "stated_goal" not in c.STREAM_FIELDS
    assert set(c.STREAM_TEXT_FIELDS) == {"stated_goal"}
