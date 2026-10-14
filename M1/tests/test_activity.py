import pandas as pd
import pytest

from kestrel_m1 import activity as ac
from kestrel_m1 import readers
from kestrel_m1.rejects import REASONS, first_reason

INT_COLS = [c for c, t in readers.EXPECTED_ACTIVITY_SCHEMA.items() if str(t) == "int64"]


def session(sid="ss_000001", t0="2026-03-01T10:00:00.000Z", **start_overrides):
    """A minimal valid session: start, one turn, one tool call, end (ag_007 v2.3.1)."""
    base = {c: None for c in readers.EXPECTED_ACTIVITY_SCHEMA}
    base.update(session_id=sid, tenant_id="tn_003", agent_id="ag_007", release_version="2.3.1",
                sandbox_instance_id="sbx_0001", status="ok")
    t = pd.Timestamp(t0)
    iso = lambda s: (t + pd.Timedelta(seconds=s)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    rows = [
        {**base, "event_type": "session.start", "task_id": "tk_1", "task_category": "market_research",
         "expected_tool_categories": "retrieval", **start_overrides},
        {**base, "event_type": "llm.turn", "latency_ms": 900, "turn_index": 0, "model_id": "mdl_kst_pro_1",
         "content_source": "user", "stated_goal": "research", "injection_indicators": "", "input_tokens": 100,
         "output_tokens": 50, "finish_reason": "tool_call"},
        {**base, "event_type": "tool.call", "status": "success", "latency_ms": 300, "tool_id": "tl_web_fetch",
         "tool_version": "1.2.0", "retry_number": 0, "target_resource": "api.kestrel.ai"},
        {**base, "event_type": "session.end", "latency_ms": 5000},
    ]
    for i, r in enumerate(rows):
        r.setdefault("event_id", None)
        r["event_id"] = r["event_id"] or f"ev_{sid[3:]}{i}"
        r["sequence_number"] = i + 1
        r["event_time"] = r["event_time"] or iso(i)
        r["ingest_time"] = r["ingest_time"] or iso(i + 1)
        if r["event_type"] == "tool.call":
            r["finished_at"] = iso(i + 0.3)
    return rows


def frame(rows):
    df = pd.DataFrame(rows, columns=list(readers.EXPECTED_ACTIVITY_SCHEMA))
    for c in INT_COLS:
        df[c] = df[c].astype("Int64")
    df.insert(0, "source_line", range(1, len(df) + 1))
    df.insert(0, "source_file", "agent_activity.parquet")
    return df


def run(rows, ref, **kw):
    raw = frame(rows)
    res = ac.clean_activity(raw, ref, **kw)
    reason = pd.Series(pd.NA, index=raw.source_line, dtype="string")
    reason[res.rejects.source_line.to_numpy()] = res.rejects.reason.to_numpy()
    return res, reason


def test_clean_session_is_fully_accepted_with_task_and_agent_sk(ref):
    res, reason = run(session(), ref)
    assert reason.isna().all() and len(res.accepted) == 4
    a = res.accepted
    assert (a.task_id == "tk_1").all()                                   # propagated from the start row
    assert a.agent_sk.nunique() == 1 and a.agent_sk.notna().all()
    assert (a.quality_flags == "").all()


def test_duplicate_keeps_earliest_ingest(ref):
    rows = session()
    late_copy = {**rows[1], "ingest_time": "2026-03-01T10:05:00.000Z"}
    res, reason = run([late_copy] + rows, ref)                           # later copy comes first in the file
    assert reason[1] == "DUPLICATE_EVENT_ID" and reason[2:].isna().all()
    assert "line 3" in res.rejects.reason_detail[0]


def test_first_reason_in_enum_order_wins(ref):
    rows = session()
    rows[1].update(agent_id="ag_999", release_version="9.9.9", event_time="not-a-time", status=None)
    _, reason = run(rows, ref)
    assert reason[2] == "UNKNOWN_AGENT_ID"                               # also malformed + missing status


def test_rejected_start_turns_its_session_into_orphans(ref):
    rows = session(tenant_id="tn_777")                                   # only the start row is bad
    res, reason = run(rows, ref)
    assert reason[1] == "UNKNOWN_TENANT_ID"
    assert (reason[2:] == "ORPHAN_EVENT").all()
    assert res.rejects.reason_detail.str.contains("session.start of this session was rejected").sum() == 3


def test_orphan_outranks_invalid_event_type(ref):
    rows = session()[1:]                                                 # no session.start at all
    rows.append({**rows[0], "event_id": "ev_x1", "event_type": "agent.telepathy", "sequence_number": None})
    _, reason = run(rows, ref)
    assert (reason == "ORPHAN_EVENT").all()


def test_invalid_event_type_inside_a_valid_session(ref):
    rows = session()
    rows.append({**rows[1], "event_id": "ev_x2", "event_type": "llm.thought", "sequence_number": None})
    _, reason = run(rows, ref)
    assert reason[5] == "INVALID_EVENT_TYPE" and reason[:4].isna().all()


def test_missing_and_negative_fields(ref):
    rows = session()
    rows[2]["status"] = None
    rows[1]["input_tokens"] = -5
    rows[1]["output_tokens"] = None                                      # missing-token policy: keep
    res, reason = run(rows, ref)
    assert reason[3] == "MISSING_REQUIRED_FIELD" and reason[2] == "INVALID_NUMERIC_RANGE"
    rows = session()
    rows[1]["output_tokens"] = None
    res, reason = run(rows, ref)
    assert reason.isna().all()
    assert "OUTPUT_TOKENS_MISSING" in res.accepted.set_index("event_type").loc["llm.turn", "quality_flags"]


def test_repairs_are_kept_and_flagged(ref):
    rows = session()
    rows[0]["release_version"] = "v2.3.1"
    rows[1]["event_time"] = "1772359201000"                              # epoch ms = 2026-03-01T10:00:01Z
    rows[2].update(tool_id="http_get", status="succeeded")
    res, reason = run(rows, ref)
    assert reason.isna().all()
    a = res.accepted.set_index("event_type")
    assert "VERSION_PREFIX_STRIPPED" in a.loc["session.start", "quality_flags"]
    assert a.loc["session.start", "release_version"] == "2.3.1"
    assert "NONSTANDARD_TIMESTAMP_FORMAT" in a.loc["llm.turn", "quality_flags"]
    assert a.loc["llm.turn", "event_time_utc"] == pd.Timestamp("2026-03-01T10:00:01Z")
    assert a.loc["tool.call", "tool_id"] == "tl_web_fetch" and a.loc["tool.call", "status"] == "success"
    assert {"TOOL_ALIAS_RESOLVED", "STATUS_NORMALISED"} <= set(a.loc["tool.call", "quality_flags"].split("|"))


def test_late_is_flagged_in_batch_and_rejected_only_with_a_watermark(ref):
    rows = session()
    rows[1]["ingest_time"] = "2026-03-01T10:01:30.000Z"                  # 89 s late
    res, reason = run(rows, ref)
    assert reason.isna().all() and res.accepted.is_late.sum() == 1
    _, reason = run(rows, ref, watermark_s=60)
    assert reason[2] == "LATE_BEYOND_WATERMARK"


def test_reason_names_are_closed():
    with pytest.raises(ValueError):
        first_reason({"NOT_A_REASON": (pd.Series([True]), "x")}, pd.RangeIndex(1))
    assert len(REASONS) == 11


def test_full_pack_checkpoint(pack):
    res = pack.act
    counts = dict(zip(res.summary.outcome, res.summary.rows))
    assert sum(counts.values()) == 937_752
    assert counts == {"ACCEPTED": 924_775, "DUPLICATE_EVENT_ID": 7_427, "UNKNOWN_AGENT_ID": 1_332,
                      "UNKNOWN_TENANT_ID": 730, "MALFORMED_TIMESTAMP": 708, "ORPHAN_EVENT": 200,
                      "INVALID_EVENT_TYPE": 302, "MISSING_REQUIRED_FIELD": 730, "INVALID_NUMERIC_RANGE": 1_548}
