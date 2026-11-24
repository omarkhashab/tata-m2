"""Lab 4 contracts: one Pydantic model per record type, checked where data enters.

The models are the machine-readable form of the Dataset Description (sections 3.1 to
3.11). They are used at two entry points:

* the **file readers** (``gate.py``): every record of every batch file is validated
  before cleaning starts;
* the **Kafka consumer** (``stream/consumer.py``): every message is validated before
  it can reach the curated topic or the warehouse.

A failed validation never just raises: ``check()`` turns Pydantic's error list into
**exactly one** reason from the closed rejection enum (Dataset Description section 4),
choosing the first applicable reason in enum order, the same priority M1's reject
ladder uses (``kestrel_m1.rejects.REASONS``). The mapping from Pydantic error type to
enum reason is ``reason_of_error``:

==============================================  =========================
Pydantic error                                  enum reason
==============================================  =========================
key missing, or a required field is null        MISSING_REQUIRED_FIELD
custom ``malformed_timestamp``                  MALFORMED_TIMESTAMP
``union_tag_invalid`` on ``event_type``         INVALID_EVENT_TYPE
``greater_than_equal`` / ``less_than_equal`` ... INVALID_NUMERIC_RANGE
anything else (enum, pattern, wrong type)       SCHEMA_VIOLATION
==============================================  =========================

Cross-record reasons (DUPLICATE_EVENT_ID, UNKNOWN_*_ID, ORPHAN_EVENT,
LATE_BEYOND_WATERMARK) need state (other records, the dimensions, a watermark), so they
are decided by the stage that holds that state, never by a single-record model.

Repairs that M1 makes instead of rejecting (``succeeded`` -> ``success``, an invalid
tenant region -> null) are expressed in the models as normalising validators, and every
repair is reported through the validation context (``check(..., repairs=[])``), so
nothing is fixed silently.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import (BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter, ValidationError,
                      ValidationInfo, field_validator)
from pydantic_core import PydanticCustomError

from kestrel_m1.rejects import REASONS
from kestrel_m1.timeutil import DMY_RX, EPOCH_MS_RX, ISO_RX

# --------------------------------------------------------------------------- enums
EVENT_TYPES = ("session.start", "llm.turn", "tool.call", "session.end", "session.error")
LIFECYCLE_STATUS = Literal["ok", "error"]
TOOL_STATUS = Literal["success", "failure", "denied", "timeout"]
STATUS_REPAIRS = {"succeeded": "success"}        # unambiguous synonym, same rule as M1 (AC12)
CONTENT_SOURCE = Literal["user", "system", "retrieved_web", "tool_output"]
FINISH_REASON = Literal["stop", "length", "tool_call", "content_filter"]
TASK_CATEGORY = Literal["market_research", "ticket_triage", "report_generation", "data_query", "code_review",
                        "incident_response"]
ERROR_CLASS = Literal["TIMEOUT", "AUTH_DENIED", "RATE_LIMIT", "POLICY_DENIED", "TARGET_UNREACHABLE", "INVALID_ARGS"]
INJECTION_INDICATORS = {"instruction_override", "role_confusion", "exfil_request", "encoded_payload",
                        "tool_coercion", "policy_probe"}

RANGE_ERRORS = {"greater_than", "greater_than_equal", "less_than", "less_than_equal"}

_ISO = re.compile(ISO_RX)
_ISO_OFFSET = re.compile(ISO_RX.replace("(?:Z|", "(?:"))         # explicit +hh:mm required (SIEM)
_EPOCH_MS = re.compile(EPOCH_MS_RX)
_DMY = re.compile(DMY_RX)
_SYSLOG = re.compile(r"[A-Z][a-z]{2} [ \d]\d \d{2}:\d{2}:\d{2}")
IPV4 = r"^(?:\d{1,3}\.){3}\d{1,3}$"


# ---------------------------------------------------------------------- timestamps

def _malformed(value: Any, why: str) -> PydanticCustomError:
    return PydanticCustomError("malformed_timestamp", "malformed timestamp {value!r}: {why}",
                               {"value": value, "why": why})


def _iso(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as e:                       # impossible calendar date such as 2026-02-30
        raise _malformed(value, str(e)) from None


def event_time_any(value: Any) -> Any:
    """Activity ``event_time``: ISO-8601, epoch milliseconds (string or JSON int) or
    ``dd/mm/yyyy HH:MM:SS``, all UTC (M1 finding AC1/AC2). Anything else is malformed."""
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, bool):
        raise _malformed(value, "boolean")
    if isinstance(value, int):
        value = str(value)
    if not isinstance(value, str):
        raise _malformed(value, f"type {type(value).__name__}")
    if _ISO.fullmatch(value):
        return _iso(value)
    if _EPOCH_MS.fullmatch(value):
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
    if _DMY.fullmatch(value):
        try:
            return datetime.strptime(value, "%d/%m/%Y %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError as e:
            raise _malformed(value, str(e)) from None
    raise _malformed(value, "not ISO-8601, epoch-ms or dd/mm/yyyy")


def iso_time(value: Any) -> Any:
    """Strict ISO-8601 with ``Z`` or an offset (``ingest_time``, ``finished_at``, flows, policies)."""
    if value is None or isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not _ISO.fullmatch(value):
        raise _malformed(value, "not ISO-8601 with an offset")
    return _iso(value)


def iso_offset_time(value: Any) -> Any:
    """SIEM ``detected_at``: ISO-8601 that must carry an explicit ``+hh:mm`` offset."""
    if value is None or isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not _ISO_OFFSET.fullmatch(value):
        raise _malformed(value, "needs an explicit +hh:mm offset")
    return _iso(value)


def syslog_time(value: Any) -> Any:
    """Audit ``log_time`` (``Mar 14 11:22:04``): host-local, no year. Kept naive here;
    the audit stage converts it with the measured per-host offset (M1 AU4)."""
    if value is None or isinstance(value, datetime):
        return value
    if value == "-":
        return None
    if not isinstance(value, str) or not _SYSLOG.fullmatch(value):
        raise _malformed(value, "not 'Mon DD HH:MM:SS'")
    try:
        return datetime.strptime(f"2026 {value}", "%Y %b %d %H:%M:%S")
    except ValueError as e:
        raise _malformed(value, str(e)) from None


EventTime = Annotated[datetime, BeforeValidator(event_time_any)]
IsoTime = Annotated[datetime, BeforeValidator(iso_time)]
IsoOffsetTime = Annotated[datetime, BeforeValidator(iso_offset_time)]
SyslogTime = Annotated[datetime, BeforeValidator(syslog_time)]
NonNegInt = Annotated[int, Field(ge=0)]
Port = Annotated[int, Field(ge=1, le=65535)]


def _dash_is_null(v: Any) -> Any:
    """The syslog null marker ``-`` (Dataset Description section 0)."""
    return None if v == "-" else v


def _pack_bool(v: Any) -> Any:
    """Pack booleans are lowercase ``true``/``false`` in CSV."""
    return {"true": True, "false": False}.get(v, v) if isinstance(v, str) else v


def _empty_is_null(v: Any) -> Any:
    return None if v == "" else v


PackBool = Annotated[bool, BeforeValidator(_pack_bool)]
Dash = BeforeValidator(_dash_is_null)


def _repair(info: ValidationInfo, what: str) -> None:
    if info.context is not None and "repairs" in info.context:
        info.context["repairs"].append(what)


# ===================================================================== activity events

class _ActivityBase(BaseModel):
    """Fields every activity row has (Dataset Description 3.5, "FILLED ON: all") plus the
    optional type-specific ones. Ranges apply whenever a value is present."""
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=False)

    event_id: Annotated[str, Field(pattern=r"^ev_\w+$")]
    event_time: EventTime
    ingest_time: IsoTime
    sequence_number: Annotated[int, Field(ge=1)]
    session_id: Annotated[str, Field(pattern=r"^ss_\w+$")]
    tenant_id: str
    agent_id: str
    release_version: str
    sandbox_instance_id: str

    latency_ms: Optional[NonNegInt] = None
    turn_index: Optional[NonNegInt] = None
    model_id: Optional[str] = None
    content_source: Optional[CONTENT_SOURCE] = None
    stated_goal: Optional[str] = None
    injection_indicators: Optional[str] = None
    input_tokens: Optional[NonNegInt] = None
    output_tokens: Optional[NonNegInt] = None          # not required: missing-token policy keeps the turn
    finish_reason: Optional[FINISH_REASON] = None
    tool_id: Optional[str] = None
    tool_version: Optional[str] = None
    finished_at: Optional[IsoTime] = None
    retry_number: Optional[NonNegInt] = None
    error_class: Optional[ERROR_CLASS] = None
    target_resource: Optional[str] = None
    bytes_out: Optional[NonNegInt] = None
    task_id: Optional[str] = None
    task_category: Optional[TASK_CATEGORY] = None
    expected_tool_categories: Optional[str] = None


class SessionStart(_ActivityBase):
    event_type: Literal["session.start"]
    status: LIFECYCLE_STATUS
    task_id: str
    task_category: TASK_CATEGORY
    expected_tool_categories: str


class LlmTurn(_ActivityBase):
    event_type: Literal["llm.turn"]
    status: LIFECYCLE_STATUS
    latency_ms: NonNegInt
    turn_index: NonNegInt
    model_id: str
    content_source: CONTENT_SOURCE
    stated_goal: str
    input_tokens: NonNegInt
    finish_reason: FINISH_REASON


class ToolCall(_ActivityBase):
    event_type: Literal["tool.call"]
    status: TOOL_STATUS
    latency_ms: NonNegInt
    tool_id: str
    tool_version: str
    finished_at: IsoTime
    retry_number: NonNegInt
    target_resource: str

    @field_validator("status", mode="before")
    @classmethod
    def _synonym(cls, v: Any, info: ValidationInfo) -> Any:
        if v in STATUS_REPAIRS:
            _repair(info, "STATUS_NORMALISED")
            return STATUS_REPAIRS[v]
        return v


class SessionEnd(_ActivityBase):
    event_type: Literal["session.end"]
    status: LIFECYCLE_STATUS
    latency_ms: NonNegInt


class SessionError(_ActivityBase):
    event_type: Literal["session.error"]
    status: LIFECYCLE_STATUS
    latency_ms: NonNegInt


ActivityEvent = Annotated[Union[SessionStart, LlmTurn, ToolCall, SessionEnd, SessionError],
                          Field(discriminator="event_type")]


class AnyActivity(_ActivityBase):
    """Fallback used only to collect the *other* errors of a row whose event_type is
    invalid, so that the first reason in enum order wins exactly as in M1 (e.g. a
    malformed timestamp outranks an invalid event type)."""
    event_type: str
    status: str


class EndOfStream(BaseModel):
    """The control record that terminates ``stream_events.json`` (Dataset Description 3.11)."""
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["EOS"]
    event_time: IsoTime
    producer_id: str


# ======================================================================= other sources

class NetworkFlow(BaseModel):
    """network_flows.csv (3.6). CSV cells arrive as text; ints are parsed by the model."""
    model_config = ConfigDict(extra="ignore")
    flow_id: Annotated[str, Field(pattern=r"^nf_\w+$")]
    session_id: Annotated[str, Field(pattern=r"^ss_\w+$")]
    sandbox_instance_id: str
    flow_time: IsoTime
    src_ip: Annotated[str, Field(pattern=IPV4)]
    dst_host: Optional[str] = None                      # nullable: repaired from the IP map (M1 NF2)
    dst_ip: Annotated[str, Field(pattern=IPV4)]
    dst_port: Port
    protocol: Literal["https", "http", "ssh", "smb", "dns", "rdp"]
    bytes_sent: NonNegInt
    bytes_received: NonNegInt
    verdict: Literal["allowed", "blocked", "logged_only"]
    geo_country: Optional[str] = None


class AuditDecision(BaseModel):
    """One regex-parsed sandbox_audit.log line (3.7). ``-`` is the null marker."""
    model_config = ConfigDict(extra="ignore")
    log_time: SyslogTime
    host: Annotated[str, Dash]
    process: Annotated[str, Dash]
    pid: Annotated[NonNegInt, Dash]
    session_id: Annotated[str, Dash]
    sandbox_instance_id: Annotated[str, Dash]
    decision: Annotated[Literal["ALLOW", "DENY"], Dash]
    action: Annotated[Literal["socket_connect", "file_read", "file_write", "process_spawn", "setuid", "mount",
                              "ptrace"], Dash]
    target: Annotated[str, Dash]
    reason: Annotated[str, Dash]
    policy_version: Annotated[Optional[Annotated[int, Field(ge=1)]], Dash] = None


class SiemDestination(BaseModel):
    host: Annotated[str, Field(pattern=IPV4)]           # Dataset Description 3.8: an IP address
    port: Port


class SiemTechnique(BaseModel):
    id: str
    name: Optional[str] = None


class SiemAlert(BaseModel):
    """One element of the ``alerts`` array in siem_alerts.json (3.8), nested as exported."""
    model_config = ConfigDict(extra="ignore")
    alert_id: Annotated[str, Field(pattern=r"^al_\w+$")]
    source_system: str
    severity: Literal["low", "medium", "high", "critical"]
    detected_at: IsoOffsetTime
    source_ip: Annotated[str, Field(pattern=IPV4)]
    destination: SiemDestination
    technique: SiemTechnique
    rule: str
    confidence: Annotated[float, Field(ge=0, le=1)]


class SiemExport(BaseModel):
    generated_at: IsoOffsetTime
    source: str
    alerts: list[dict]


# ------------------------------------------------------------------- reference data

class Tenant(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tenant_id: Annotated[str, Field(pattern=r"^tn_\w+$")]
    tenant_name: str
    industry: Literal["finance", "healthcare", "logistics", "retail", "public_sector"]
    plan_tier: Literal["starter", "growth", "enterprise"]
    region: Optional[Literal["eu-west-1", "us-east-1", "me-south-1"]] = None
    onboarded_at: IsoTime
    is_active: PackBool
    security_contact: Optional[str] = None              # nullable (M1 T3)

    @field_validator("region", mode="before")
    @classmethod
    def _region(cls, v: Any, info: ValidationInfo) -> Any:
        if v is not None and v not in ("eu-west-1", "us-east-1", "me-south-1"):
            _repair(info, "INVALID_ENUM_REGION")         # M1 T2: kept as null + region_raw, not rejected
            return None
        return v


class Agent(BaseModel):
    model_config = ConfigDict(extra="ignore")
    agent_id: Annotated[str, Field(pattern=r"^ag_\w+$")]
    release_version: Annotated[str, Field(pattern=r"^\d+\.\d+\.\d+$")]
    agent_name: str
    agent_type: Literal["research", "support", "ops", "analytics", "coding"]
    model_id: str
    tenant_id: Annotated[str, Field(pattern=r"^tn_\w+$")]
    sandbox_profile_id: Annotated[str, Field(pattern=r"^sp_\w+$")]
    owner_team: str
    autonomy_level: Literal["supervised", "semi_autonomous", "autonomous"]
    max_tool_calls_per_session: NonNegInt
    launch_date: date
    is_deprecated: PackBool

    @field_validator("release_version", mode="before")
    @classmethod
    def _strip_v(cls, v: Any, info: ValidationInfo) -> Any:
        if isinstance(v, str) and v[:1] in ("v", "V"):
            _repair(info, "VERSION_PREFIX_STRIPPED")     # M1 AG1 / AC8
            return v[1:]
        return v


class Tool(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tool_id: Annotated[str, Field(pattern=r"^tl_\w+$")]
    tool_version: Annotated[str, Field(pattern=r"^\d+\.\d+\.\d+$")]
    tool_name: str
    tool_category: Literal["retrieval", "database", "shell", "network", "filesystem", "messaging", "code_exec"]
    risk_tier: Literal["low", "medium", "high", "critical"]
    requires_approval: PackBool
    owner_team: str


class ModelPrice(BaseModel):
    model_config = ConfigDict(extra="ignore")
    model_id: str
    model_family: str
    input_price_per_1k: Optional[Annotated[float, Field(ge=0)]] = None     # nullable: M1 P1, cost stays null
    output_price_per_1k: Optional[Annotated[float, Field(ge=0)]] = None
    currency: Literal["USD"]
    effective_from: IsoTime
    effective_to: Optional[IsoTime] = None


class PolicyNetwork(BaseModel):
    egress_allowlist: list[str]
    denied_ports: list[Port]
    max_egress_mb: Annotated[float, Field(ge=0)]


class PolicyFilesystem(BaseModel):
    read_scope: list[str]
    write_scope: list[str]


class PolicyResources(BaseModel):
    max_runtime_s: NonNegInt
    max_memory_mb: NonNegInt


class SandboxPolicy(BaseModel):
    """One policy version (3.4). ``network`` may be absent: then no egress is allowed."""
    model_config = ConfigDict(extra="forbid")
    sandbox_profile_id: Annotated[str, Field(pattern=r"^sp_\w+$")]
    policy_version: Annotated[int, Field(ge=1)]
    valid_from: IsoTime
    valid_to: Optional[IsoTime] = None
    allowed_tools: list[Annotated[str, Field(pattern=r"^tl_\w+$")]]
    network: Optional[PolicyNetwork] = None
    filesystem: PolicyFilesystem
    resources: PolicyResources
    escalation_allowed: bool


class IncidentLabel(BaseModel):
    """incident_labels.csv (3.10): held-out ground truth, read only by the evaluation step."""
    model_config = ConfigDict(extra="ignore")
    label_id: str
    session_id: Annotated[str, Field(pattern=r"^ss_\w+$")]
    label: Literal["benign", "suspicious", "confirmed_rogue"]
    attack_stage: Annotated[Optional[Literal["injection", "recon", "privilege_escalation", "lateral_movement",
                                             "exfiltration"]], BeforeValidator(_empty_is_null)] = None
    technique_ids: Annotated[Optional[str], BeforeValidator(_empty_is_null)] = None
    labelled_by: Literal["analyst", "automated", "red_team"]
    labelled_at: IsoTime
    notes: Optional[str] = None


# ===================================================================== error -> reason

def reason_of_error(err: dict) -> str:
    """Map one Pydantic error (``ValidationError.errors()`` item) to an enum reason."""
    t = err["type"]
    if t == "missing" or t == "union_tag_not_found":
        return "MISSING_REQUIRED_FIELD"
    if t == "malformed_timestamp":
        return "MALFORMED_TIMESTAMP"
    if t == "union_tag_invalid":
        return "INVALID_EVENT_TYPE"
    if err.get("input", 0) is None:                     # a required field that is present but null
        return "MISSING_REQUIRED_FIELD"
    if t in RANGE_ERRORS:
        return "INVALID_NUMERIC_RANGE"
    return "SCHEMA_VIOLATION"


def _loc(err: dict) -> str:
    loc = [str(p) for p in err["loc"] if not (isinstance(p, str) and p in EVENT_TYPES)]
    return ".".join(loc) or "<record>"


@dataclass
class Verdict:
    """Outcome of one contract check: a validated model or exactly one enum reason."""
    value: Any = None
    reason: Optional[str] = None
    detail: Optional[str] = None
    repairs: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.reason is None


def first_reason(errors: list[dict]) -> tuple[Optional[str], Optional[str]]:
    """The first applicable enum reason, and a detail listing the fields behind it."""
    if not errors:
        return None, None
    by_reason: dict[str, list[str]] = {}
    for e in errors:
        by_reason.setdefault(reason_of_error(e), []).append(_loc(e))
    reason = min(by_reason, key=REASONS.index)
    fields = sorted(dict.fromkeys(by_reason[reason]))
    return reason, f"{reason.lower()}: {','.join(fields)}"


_ADAPTERS: dict[Any, TypeAdapter] = {}


def adapter(model) -> TypeAdapter:
    if model not in _ADAPTERS:
        _ADAPTERS[model] = TypeAdapter(model)
    return _ADAPTERS[model]


def check(model, record: Any) -> Verdict:
    """Validate one record against ``model``; never raises for bad data."""
    repairs: list[str] = []
    try:
        value = adapter(model).validate_python(record, context={"repairs": repairs})
        return Verdict(value=value, repairs=repairs)
    except ValidationError as e:
        errors = e.errors(include_url=False)
    if model is ActivityEvent and isinstance(record, dict) and record.get("event_type") is None:
        # a null tag is a missing required field, not an invalid event type (same as M1)
        errors = [dict(x, type="missing", loc=("event_type",)) if x["type"].startswith("union_tag") else x
                  for x in errors]
    if model is ActivityEvent and any(x["type"] == "union_tag_invalid" for x in errors) and isinstance(record, dict):
        # an unknown event_type stops the union at the tag; collect the row's other errors too
        try:
            AnyActivity.model_validate(record)
        except ValidationError as e2:
            errors += [x for x in e2.errors(include_url=False) if x["loc"] != ("event_type",)]
    reason, detail = first_reason(errors)
    return Verdict(reason=reason, detail=detail, repairs=repairs, errors=errors)


def check_activity(record: Any) -> Verdict:
    """An activity row / stream message. A non-object is UNPARSEABLE_RECORD."""
    if not isinstance(record, dict):
        return Verdict(reason="UNPARSEABLE_RECORD", detail=f"not a JSON object ({type(record).__name__})")
    return check(ActivityEvent, record)


# record type -> model, used by the file-reader gate and the docs
MODELS = {
    "tenants.csv": Tenant,
    "agents.csv": Agent,
    "tools.csv": Tool,
    "model_pricing.csv": ModelPrice,
    "sandbox_policies.json": SandboxPolicy,
    "agent_activity.parquet": ActivityEvent,
    "network_flows.csv": NetworkFlow,
    "sandbox_audit.log": AuditDecision,
    "siem_alerts.json": SiemAlert,
    "incident_labels.csv": IncidentLabel,
    "stream_events.json": ActivityEvent,                # plus EndOfStream for the control record
}

# ------------------------------------------------------------------ stream sanitising
# Raw prompt/response text never goes on the stream. The only free-text field in the
# activity contract is ``stated_goal`` (the goal the LLM articulated, i.e. model output).
# The producer forwards an explicit allowlist of operational fields; stated_goal is
# replaced by a one-way fingerprint so goal *changes* stay detectable without the text.
STREAM_TEXT_FIELDS = {"stated_goal"}
STREAM_FIELDS = [
    "event_id", "event_time", "ingest_time", "sequence_number", "event_type", "session_id", "tenant_id",
    "agent_id", "release_version", "sandbox_instance_id", "status", "latency_ms", "turn_index", "model_id",
    "content_source", "injection_indicators", "input_tokens", "output_tokens", "finish_reason", "tool_id",
    "tool_version", "finished_at", "retry_number", "error_class", "target_resource", "bytes_out", "task_id",
    "task_category", "expected_tool_categories",
]
EOS_FIELDS = ["event_type", "event_time", "producer_id"]


def utc_iso(ts: datetime) -> str:
    """Canonical UTC text form: ``2026-03-14T09:22:01.412Z`` (Dataset Description section 0)."""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def to_utc_seconds(ts: datetime) -> float:
    return (ts - datetime(1970, 1, 1, tzinfo=timezone.utc)) / timedelta(seconds=1)
