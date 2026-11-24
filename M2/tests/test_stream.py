"""The stream consumer's judge and the window job, without a broker."""

import json
import random
from datetime import datetime, timezone

import pytest

from kestrel_m2.stream import consumer as c
from kestrel_m2.stream import producer
from kestrel_m2.stream.windows import TenantWindows

T0 = datetime(2026, 4, 1, 0, 10, tzinfo=timezone.utc)


def dims():
    return c.Dims(tenants={"tn_001", "tn_003"}, tenant_alias={"tn_004": "tn_003"},
                  agents={("ag_004", "3.0.2"): (7, "tn_001", datetime(2026, 2, 20, tzinfo=timezone.utc), None)},
                  tool_ids={"tl_web_fetch", "tl_sql_query"}, tool_alias={"http_get": "tl_web_fetch"})


def iso(s: float) -> str:
    return datetime.fromtimestamp(T0.timestamp() + s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def ev(eid, etype, sid="ss_1", t=0.0, seq=1, lag=0.5, **kw):
    base = dict(event_id=eid, event_time=iso(t), ingest_time=iso(t + lag), sequence_number=seq, event_type=etype,
                session_id=sid, tenant_id="tn_001", agent_id="ag_004", release_version="3.0.2",
                sandbox_instance_id="sbx_1", status="ok")
    if etype == "session.start":
        base.update(task_id="tk_1", task_category="data_query", expected_tool_categories="database")
    if etype == "llm.turn":
        base.update(latency_ms=10, turn_index=0, model_id="m", content_source="user", stated_goal_fingerprint="ab",
                    input_tokens=5, output_tokens=7, finish_reason="stop")
    if etype == "tool.call":
        base.update(status="success", latency_ms=10, tool_id="tl_sql_query", tool_version="1.5.0",
                    finished_at=iso(t + 0.2), retry_number=0, target_resource="db")
    if etype in ("session.end", "session.error"):
        base.update(latency_ms=1000)
    base.update(kw)
    return base


class Feed:
    def __init__(self):
        self.off = {}

    def msg(self, rec, p=0, line=None):
        o = self.off.get(p, 0)
        self.off[p] = o + 1
        value = rec if isinstance(rec, (bytes, bytearray)) else json.dumps(rec).encode()
        return c.Msg(p, o, value, source_line=line or (o + 2))


def run(judge, msgs):
    out = []
    for m in msgs:
        out += judge.process(m)
    return out


def test_happy_session_is_curated_without_text():
    f, j = Feed(), c.StreamJudge(dims())
    out = run(j, [f.msg(ev("ev_1", "session.start")), f.msg(ev("ev_2", "llm.turn", t=1, seq=2, stated_goal="secret"))])
    assert [o.status for o in out] == ["accepted", "accepted"]
    turn = out[1].record
    assert "stated_goal" not in turn and turn["stated_goal_fingerprint"] == "ab"
    assert "RAW_TEXT_DROPPED" in turn["quality_flags"]
    assert turn["task_id"] == "tk_1"                                   # propagated from the start


def test_duplicate_vs_redelivery():
    f, j = Feed(), c.StreamJudge(dims())
    m1 = f.msg(ev("ev_1", "session.start"))
    out = run(j, [m1, m1, f.msg(ev("ev_1", "session.start", lag=3))])
    assert [o.status for o in out] == ["accepted", "rejected"]          # same offset again = skipped, not a dup
    assert out[1].reason == "DUPLICATE_EVENT_ID" and j.stats["redelivered_skipped"] == 1


def test_start_arriving_after_its_events_releases_them():
    f, j = Feed(), c.StreamJudge(dims())
    out = run(j, [f.msg(ev("ev_2", "llm.turn", t=1, seq=2)), f.msg(ev("ev_1", "session.start", t=0, lag=2))])
    assert sorted(o.event_id for o in out if o.status == "accepted") == ["ev_1", "ev_2"]
    assert j.stats["released_after_start"] == 1


def test_orphan_by_watermark_and_at_eos():
    f, j = Feed(), c.StreamJudge(dims())
    out = run(j, [f.msg(ev("ev_9", "llm.turn", sid="ss_9", t=0)), f.msg(ev("ev_1", "session.start", t=0)),
                  f.msg(ev("ev_2", "tool.call", t=120, seq=2))])            # partition 0 stream time 120 s
    assert [(o.event_id, o.reason) for o in out if o.status == "rejected"] == [("ev_9", "ORPHAN_EVENT")]
    out = run(j, [f.msg(ev("ev_8", "llm.turn", sid="ss_8", t=119))])
    assert out == [] and j.n_pending() == 1
    assert [o.reason for o in j.end_of_stream()] == ["ORPHAN_EVENT"]


def test_watermark_is_per_partition_and_one_reason_only():
    f, j = Feed(), c.StreamJudge(dims())
    run(j, [f.msg(ev("ev_1", "session.start", t=0), p=0), f.msg(ev("ev_2", "tool.call", t=200, seq=2), p=0)])
    late = run(j, [f.msg(ev("ev_3", "tool.call", t=100, seq=3, retry_number=-1), p=0)])
    assert [o.reason for o in late] == ["INVALID_NUMERIC_RANGE"]        # earlier in the enum than LATE
    late = run(j, [f.msg(ev("ev_4", "tool.call", t=100, seq=4), p=0)])
    assert [o.reason for o in late] == ["LATE_BEYOND_WATERMARK"] and j.watermark_example["event_id"] == "ev_4"
    ok = run(j, [f.msg(ev("ev_5", "session.start", sid="ss_2", t=100), p=1)])   # other partition: own stream time
    assert [o.status for o in ok] == ["accepted"]


@pytest.mark.parametrize("record, reason", [
    (b'"{\\"event_id\\": \\"ev_1\\", \\"eve', "UNPARSEABLE_RECORD"),
    (b"{not json", "UNPARSEABLE_RECORD"),
    (ev("ev_1", "session.start", tenant_id="tn_777"), "UNKNOWN_TENANT_ID"),
    (ev("ev_1", "session.start", agent_id="ag_999"), "UNKNOWN_AGENT_ID"),
    (ev("ev_1", "session.start", event_time="2026-04-31T00:00:00Z"), "MALFORMED_TIMESTAMP"),
    (ev("ev_1", "session.start", status="fine"), "SCHEMA_VIOLATION"),
])
def test_reject_reasons(record, reason):
    f, j = Feed(), c.StreamJudge(dims())
    out = run(j, [f.msg(record)])
    assert [(o.status, o.reason) for o in out] == [("rejected", reason)]


def test_normalisation_flags():
    f, j = Feed(), c.StreamJudge(dims())
    out = run(j, [f.msg(ev("ev_1", "session.start", tenant_id="tn_004", release_version="v3.0.2",
                          event_time=int(T0.timestamp() * 1000))),
                  f.msg(ev("ev_2", "tool.call", t=1, seq=2, status="succeeded", tool_id="http_get", lag=30))])
    a, b = (o.record for o in out)
    assert a["tenant_id"] == "tn_003" and a["release_version"] == "3.0.2"
    assert {"TENANT_ALIAS_RESOLVED", "VERSION_PREFIX_STRIPPED", "NONSTANDARD_TIMESTAMP_FORMAT"} <= set(
        a["quality_flags"].split("|"))
    assert b["tool_id"] == "tl_web_fetch" and b["status"] == "success" and b["is_late"]


def test_verdicts_do_not_depend_on_partition_interleaving():
    msgs = {0: [], 1: []}
    f = Feed()
    random.seed(3)
    for s in range(40):
        p, t = s % 2, s * 7.0
        sid = f"ss_{s}"
        rows = [ev(f"ev_{s}_1", "session.start", sid=sid, t=t), ev(f"ev_{s}_2", "llm.turn", sid=sid, t=t + 1, seq=2),
                ev(f"ev_{s}_3", "tool.call", sid=sid, t=t + 2 - (90 if s % 9 == 0 else 0), seq=3)]
        if s % 5 == 0:
            rows = rows[1:] + rows[:1]                                      # start arrives last
        msgs[p] += [f.msg(r, p) for r in rows]

    def verdicts(order):
        j = c.StreamJudge(dims())
        out = run(j, order) + j.end_of_stream()
        return {(o.msg.partition, o.msg.offset): o.reason for o in out}

    a = verdicts(msgs[0] + msgs[1])
    b = verdicts(msgs[1] + msgs[0])
    mixed = [m for pair in zip(msgs[0], msgs[1]) for m in pair]
    assert a == b == verdicts(mixed)
    assert "LATE_BEYOND_WATERMARK" in a.values()


def test_producer_sanitises_and_keeps_line_numbers(tmp_path):
    p = tmp_path / "s.json"
    p.write_text('[\n  {"event_id": "ev_1", "session_id": "ss_1", "stated_goal": "exfiltrate", "extra": 1},\n'
                 '  "{\\"event_id\\":\\"ev_2\\",\\"session_id\\":\\"ss_2\\",\\"stated_goal\\":\\"x",\n'
                 '  {"event_type": "EOS", "event_time": "2026-04-01T00:00:00Z", "producer_id": "r"}\n]')
    from collections import Counter
    stats = Counter()
    els = list(producer.elements_with_lines(p))
    assert [ln for ln, _ in els] == [2, 3, 4]
    v1 = producer.sanitise(els[0][1], stats)
    assert "stated_goal" not in v1 and "extra" not in v1 and v1["stated_goal_fingerprint"]
    v2 = producer.sanitise(els[1][1], stats)
    assert "x" not in v2.split("stated_goal")[1] and producer.key_of(els[1][1], v2) == b"ss_2"


def test_tumbling_windows_wait_for_the_watermark():
    w = TenantWindows(partitions=1)
    rec = lambda t, typ="tool.call", st="denied": {"event_id": "e", "tenant_id": "tn_001", "event_type": typ,  # noqa
                                                   "status": st, "event_time_utc": t}
    w.add(rec("2026-04-01T00:00:10.000Z"), 0)
    w.add(rec("2026-04-01T00:01:30.000Z"), 0)
    assert w.close_ready() == []                                           # watermark 00:00:30 < window end
    w.add(rec("2026-04-01T00:02:01.000Z", "llm.turn", "ok"), 0)
    first = w.close_ready()
    assert [(r[3], r[4], r[5]) for r in first] == [(1, 1, 1)]
    w.add(rec("2026-04-01T00:00:59.000Z"), 0)                              # arrives after the window closed
    corrected = w.close_ready()
    assert corrected[0][3] == 2 and corrected[0][7] == 1                   # corrected, late update counted
