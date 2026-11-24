"""Lab 7 consumer: validate, normalise, deduplicate, reject with one reason, curate.

    python -m kestrel_m2.stream.consumer                 # group kestrel-tata, auto_offset_reset=earliest
    python -m kestrel_m2.stream.consumer --offset-reset latest --group probe --idle-timeout 15

Per message (``StreamJudge.process``):

1. **parse**: not JSON, or not a JSON object -> ``UNPARSEABLE_RECORD``; the ``EOS``
   control record marks its partition finished.
2. **contract**: the Lab 4 Pydantic model (``contracts.check_activity``) gives the
   record-local reasons (malformed timestamp, invalid event type, missing field, range,
   schema).
3. **state**: ``DUPLICATE_EVENT_ID`` (event_id already seen at another offset; the
   first copy, i.e. the earliest ingest_time since the feed is ordered by it, wins),
   ``UNKNOWN_AGENT_ID`` / ``UNKNOWN_TENANT_ID`` (against the warehouse dimensions),
   ``LATE_BEYOND_WATERMARK`` (event_time more than 60 s behind the watermark). The
   watermark is kept **per partition**, like Kafka Streams' *stream time*: for each
   partition, the max event_time accepted from it so far, minus 60 s. Every session lives
   in one partition (key = session_id) and each partition is in ingest order, so a
   record's verdict depends only on the records before it in its own partition: it is
   the same however the consumer interleaves the partitions. (With one global max, a
   consumer that starts late and reads the partitions in bursts rejected up to 27 extra
   on-time records as late, because another partition had been read further.)
4. **orphans**: a non-start event whose session has no accepted ``session.start`` *yet*
   is held back (38 sessions in the feed deliver their start after other events). It is
   released when the start is accepted, and becomes ``ORPHAN_EVENT`` when the watermark
   passes its event_time (its start, which cannot be later than it, could then only be
   rejected as late) or at end of stream.
5. exactly **one reason** per refused record: the first in the closed enum order.

Accepted records are normalised (UTC ISO timestamps, tenant alias, ``v`` prefix, tool
alias, ``succeeded``, task propagated from the start, quality flags) and written to the
curated topic and ``kestrel_dw.stream_curated_event``; refused ones to the rejected
topic and ``kestrel_dw.fact_rejected_record`` (channel ``stream``) with file and line.

Delivery is **at least once**: auto-commit is off; after each batch the Kafka writes are
flushed and the database transaction commits, and only then are offsets committed. The
committed offset never passes a message that is still held back. A redelivered message
(same partition and offset, already in ``stream_message_log``) is skipped; every write
is an upsert on a natural key, so replays are harmless.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from kestrel_m1.rejects import REASONS
from kestrel_m1.timeutil import DMY_RX, EPOCH_MS_RX, ISO_RX

from .. import config, contracts

RANK = {r: i for i, r in enumerate(REASONS)}
ORPHAN_RANK = RANK["ORPHAN_EVENT"]
LATE_S = 10                                   # Appendix A: late = ingest - event > 10 s
CURATED_FIELDS = [
    "event_id", "event_type", "session_id", "sequence_number", "event_time_utc", "ingest_time_utc", "lateness_s",
    "is_late", "tenant_id", "agent_id", "release_version", "agent_sk", "sandbox_instance_id", "status", "latency_ms",
    "model_id", "content_source", "stated_goal_fingerprint", "injection_indicators", "tokens_in", "tokens_out",
    "finish_reason", "tool_id", "tool_version", "finished_at_utc", "retry_number", "error_class", "target_resource",
    "bytes_out", "task_id", "task_category", "expected_tool_categories", "quality_flags", "source_file",
    "source_line", "kafka_partition", "kafka_offset",
]


# ============================================================================ messages

@dataclass
class Msg:
    partition: int
    offset: int
    value: bytes
    source_file: str = "stream_events.json"
    source_line: int = 0
    key: Optional[bytes] = None
    topic: str = config.TOPIC_RAW

    @classmethod
    def from_kafka(cls, m) -> "Msg":
        h = {k: v.decode() for k, v in (m.headers or [])}
        return cls(m.partition, m.offset, m.value, h.get("source_file", "stream_events.json"),
                   int(h.get("source_line", 0)), m.key, m.topic)


@dataclass
class Outcome:
    msg: Msg
    status: str                          # accepted | rejected | control
    reason: Optional[str] = None
    detail: Optional[str] = None
    record: Optional[dict] = None        # curated record (accepted)
    event_id: Optional[str] = None
    session_id: Optional[str] = None


@dataclass
class Dims:
    """What the consumer needs from the warehouse dimensions."""
    tenants: set
    tenant_alias: dict
    agents: dict                         # (agent_id, version) -> (agent_sk, tenant_id, valid_from, valid_to)
    tool_ids: set
    tool_alias: dict

    @classmethod
    def from_db(cls, conn, schema: str = config.DW_SCHEMA) -> "Dims":
        with conn.cursor() as cur:
            cur.execute(f"SELECT tenant_id, merged_tenant_ids FROM {schema}.dim_tenant")
            tenants, alias = set(), {}
            for t, merged in cur.fetchall():
                tenants.add(t)
                alias.update({m: t for m in merged or []})
            cur.execute(f"SELECT agent_id, release_version, agent_sk, tenant_id, valid_from_utc, valid_to_utc "
                        f"FROM {schema}.dim_agent")
            agents = {(a, v): (sk, t, f, to) for a, v, sk, t, f, to in cur.fetchall()}
            cur.execute(f"SELECT tool_id, tool_name FROM {schema}.dim_tool")
            rows = cur.fetchall()
        names = defaultdict(set)
        for tid, name in rows:
            names[name].add(tid)
        return cls(tenants, alias, agents, {r[0] for r in rows},
                   {n: next(iter(ids)) for n, ids in names.items() if len(ids) == 1})


def normalise_version(v: str) -> str:
    v = v.strip()
    return v[1:] if v[:1] in ("v", "V") else v


def _fmt_of(raw: Any) -> str:
    import re
    if isinstance(raw, int):
        return "epoch_ms"
    if isinstance(raw, str):
        for name, rx in (("iso8601", ISO_RX), ("epoch_ms", EPOCH_MS_RX), ("dmy_hms", DMY_RX)):
            if re.fullmatch(rx, raw):
                return name
    return "unknown"


def _iso(ts: Optional[datetime]) -> Optional[str]:
    return contracts.utc_iso(ts) if ts is not None else None


@dataclass
class Pending:
    msg: Msg
    rec: dict
    verdict: contracts.Verdict
    provisional: Optional[str]           # first reason found at arrival (all rank after ORPHAN), or None
    detail: Optional[str]
    event_time: Optional[datetime]


# ============================================================================== judge

@dataclass
class StreamJudge:
    dims: Dims
    watermark_s: float = config.WATERMARK_S
    seen: dict = field(default_factory=dict)                 # event_id -> (partition, offset) of the first copy
    done: set = field(default_factory=set)                   # (partition, offset) already processed (restart)
    starts: dict = field(default_factory=dict)               # session_id -> task fields of the accepted start
    rejected_starts: set = field(default_factory=set)
    pending: dict = field(default_factory=lambda: defaultdict(list))
    partitions: int = config.TOPIC_PARTITIONS
    max_by_partition: dict = field(default_factory=dict)     # partition -> max accepted event_time
    eos_partitions: set = field(default_factory=set)
    watermark_example: Optional[dict] = None
    stats: Counter = field(default_factory=Counter)

    # ---------------------------------------------------------------- watermark
    def watermark(self, partition: int) -> Optional[datetime]:
        """Stream time of ``partition`` (max accepted event_time) minus the allowed lateness."""
        st = self.max_by_partition.get(partition)
        return None if st is None else st - timedelta(seconds=self.watermark_s)

    def pending_min_offset(self) -> dict[int, int]:
        out: dict[int, int] = {}
        for items in self.pending.values():
            for p in items:
                out[p.msg.partition] = min(out.get(p.msg.partition, p.msg.offset), p.msg.offset)
        return out

    def n_pending(self) -> int:
        return sum(len(v) for v in self.pending.values())

    # ---------------------------------------------------------------- one message
    def process(self, msg: Msg) -> list[Outcome]:
        if (msg.partition, msg.offset) in self.done:            # redelivered after a restart: already written
            self.stats["redelivered_skipped"] += 1
            return []
        self.done.add((msg.partition, msg.offset))
        try:
            rec = json.loads(msg.value)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            return [self._reject(msg, None, "UNPARSEABLE_RECORD", f"not valid JSON: {e}")]
        if isinstance(rec, dict) and rec.get("event_type") == "EOS":
            v = contracts.check(contracts.EndOfStream, rec)
            if v.ok:
                self.eos_partitions.add(msg.partition)
                return [Outcome(msg, "control", detail="EOS")]
            return [self._reject(msg, rec, v.reason, v.detail)]
        if not isinstance(rec, dict):
            return [self._reject(msg, rec, "UNPARSEABLE_RECORD",
                                 f"not a JSON object ({type(rec).__name__}): truncated or double-encoded record")]

        # on the stream the goal travels only as its fingerprint (the producer strips the text):
        # the contract's required ``stated_goal`` is satisfied by a non-null fingerprint
        checked = rec
        if "stated_goal" not in rec and "stated_goal_fingerprint" in rec:
            checked = dict(rec, stated_goal=rec["stated_goal_fingerprint"])
        v = contracts.check_activity(checked)
        reasons: dict[str, str] = {}
        if v.reason:
            reasons[v.reason] = v.detail
        eid = rec.get("event_id")
        if isinstance(eid, str):
            first = self.seen.setdefault(eid, (msg.partition, msg.offset))
            if first != (msg.partition, msg.offset):
                reasons["DUPLICATE_EVENT_ID"] = f"copy kept at partition {first[0]} offset {first[1]} (earliest ingest)"
        aid, rv, tid = rec.get("agent_id"), rec.get("release_version"), rec.get("tenant_id")
        if isinstance(aid, str) and isinstance(rv, str) and (aid, normalise_version(rv)) not in self.dims.agents:
            reasons["UNKNOWN_AGENT_ID"] = f"(agent_id, release_version) = ({aid}, {rv}) not in dim_agent"
        if isinstance(tid, str) and self.dims.tenant_alias.get(tid, tid) not in self.dims.tenants:
            reasons["UNKNOWN_TENANT_ID"] = f"tenant_id {tid} not in dim_tenant"
        et = self._event_time(rec, v)
        wm = self.watermark(msg.partition)
        if et is not None and wm is not None and et < wm:
            reasons["LATE_BEYOND_WATERMARK"] = (f"event_time {_iso(et)} < watermark {_iso(wm)} "
                                                f"(partition {msg.partition} stream time "
                                                f"{_iso(self.max_by_partition[msg.partition])} - "
                                                f"{self.watermark_s:g} s)")
        first_reason = min(reasons, key=RANK.get) if reasons else None
        sid = rec.get("session_id")
        is_start = rec.get("event_type") == "session.start"
        decided_now = is_start or sid in self.starts             # a held record may still become an orphan
        if first_reason == "LATE_BEYOND_WATERMARK" and decided_now and self.watermark_example is None:
            self.watermark_example = {                           # the first record rejected by the watermark
                "event_id": eid, "event_type": rec.get("event_type"), "session_id": sid, "tenant_id": tid,
                "event_time": _iso(et), "ingest_time": rec.get("ingest_time"),
                "partition_stream_time": _iso(self.max_by_partition[msg.partition]), "watermark": _iso(wm),
                "behind_watermark_s": round((wm - et).total_seconds(), 3), "reason": first_reason,
                "kafka_partition": msg.partition, "kafka_offset": msg.offset, "source_line": msg.source_line}

        if first_reason is not None and RANK[first_reason] < ORPHAN_RANK:
            out = [self._reject(msg, rec, first_reason, reasons[first_reason])]
            if is_start and isinstance(sid, str):
                self.rejected_starts.add(sid)
            return out + self._expire(msg.partition)
        if not is_start:
            if not isinstance(sid, str):
                return [self._reject(msg, rec, "ORPHAN_EVENT", "no session_id: cannot belong to an accepted session")]
            if sid not in self.starts:
                self.pending[sid].append(Pending(msg, rec, v, first_reason,
                                                 reasons.get(first_reason) if first_reason else None, et))
                self.stats["held_for_session_start"] += 1
                return []
        if first_reason is not None:
            if is_start and isinstance(sid, str):
                self.rejected_starts.add(sid)
            return [self._reject(msg, rec, first_reason, reasons[first_reason])] + self._expire(msg.partition)
        out = [self._accept(msg, rec, v, et)]
        if is_start:
            for p in self.pending.pop(sid, []):                  # release the session's held events
                self.stats["released_after_start"] += 1
                if p.provisional:
                    out.append(self._reject(p.msg, p.rec, p.provisional, p.detail))
                else:
                    out.append(self._accept(p.msg, p.rec, p.verdict, p.event_time))
        return out + self._expire(msg.partition)

    def end_of_stream(self) -> list[Outcome]:
        """All partitions ended: whatever is still held back has no accepted start."""
        out = []
        for sid in list(self.pending):
            for p in self.pending.pop(sid):
                why = ("session.start of this session was rejected" if sid in self.rejected_starts
                       else "no accepted session.start before end of stream")
                out.append(self._reject(p.msg, p.rec, "ORPHAN_EVENT", why))
                self.stats["orphan_at_eos"] += 1
        return out

    # ---------------------------------------------------------------- helpers
    def _event_time(self, rec: dict, v: contracts.Verdict) -> Optional[datetime]:
        if v.ok:
            return v.value.event_time
        try:
            t = contracts.event_time_any(rec.get("event_time"))
            return t if isinstance(t, datetime) else None
        except Exception:                                        # noqa: BLE001 (malformed: no time)
            return None

    def _expire(self, partition: int) -> list[Outcome]:
        """Held events of ``partition`` whose event_time the partition's watermark has passed."""
        wm = self.watermark(partition)
        if wm is None:
            return []
        out = []
        for sid in list(self.pending):
            keep = []
            for p in self.pending[sid]:
                if p.msg.partition == partition and p.event_time is not None and p.event_time < wm:
                    why = (f"session.start of this session was rejected" if sid in self.rejected_starts else
                           f"no accepted session.start; watermark {_iso(wm)} passed event_time {_iso(p.event_time)}")
                    out.append(self._reject(p.msg, p.rec, "ORPHAN_EVENT", why))
                    self.stats["orphan_by_watermark"] += 1
                else:
                    keep.append(p)
            if keep:
                self.pending[sid] = keep
            else:
                del self.pending[sid]
        return out

    def _reject(self, msg: Msg, rec: Any, reason: str, detail: Optional[str]) -> Outcome:
        self.stats[f"rejected:{reason}"] += 1
        eid = rec.get("event_id") if isinstance(rec, dict) and isinstance(rec.get("event_id"), str) else None
        sid = rec.get("session_id") if isinstance(rec, dict) and isinstance(rec.get("session_id"), str) else None
        return Outcome(msg, "rejected", reason, detail, event_id=eid, session_id=sid)

    def _accept(self, msg: Msg, rec: dict, v: contracts.Verdict, et: datetime) -> Outcome:
        m = v.value
        flags = []
        tenant = self.dims.tenant_alias.get(m.tenant_id, m.tenant_id)
        version = normalise_version(m.release_version)
        sk, agent_tenant, vfrom, vto = self.dims.agents[(m.agent_id, version)]
        tool_id = m.tool_id
        if tool_id is not None and tool_id not in self.dims.tool_ids and tool_id in self.dims.tool_alias:
            tool_id = self.dims.tool_alias[tool_id]
            flags.append("TOOL_ALIAS_RESOLVED")
        lateness = (m.ingest_time - et).total_seconds()
        fmt = _fmt_of(rec.get("event_time"))
        if fmt != "iso8601":
            flags.append("NONSTANDARD_TIMESTAMP_FORMAT")
        if version != m.release_version:
            flags.append("VERSION_PREFIX_STRIPPED")
        if tenant != m.tenant_id:
            flags.append("TENANT_ALIAS_RESOLVED")
        if "STATUS_NORMALISED" in v.repairs:
            flags.append("STATUS_NORMALISED")
        if lateness > LATE_S:
            flags.append("LATE")
        if m.event_type == "llm.turn" and m.output_tokens is None:
            flags.append("OUTPUT_TOKENS_MISSING")
        if m.finished_at is not None and m.finished_at < et:
            flags.append("FINISHED_BEFORE_START")
        if et < vfrom or (vto is not None and et >= vto):
            flags.append("AGENT_VERSION_OUTSIDE_WINDOW")
        if tenant != agent_tenant:
            flags.append("TENANT_DIFFERS_FROM_AGENT")
        if "stated_goal" in rec:                                 # defence in depth: text is never curated
            flags.append("RAW_TEXT_DROPPED")
        if m.event_type == "session.start":
            self.starts[m.session_id] = {"task_id": m.task_id, "task_category": m.task_category,
                                         "expected_tool_categories": m.expected_tool_categories}
        task = self.starts.get(m.session_id, {})
        record = {
            "event_id": m.event_id, "event_type": m.event_type, "session_id": m.session_id,
            "sequence_number": m.sequence_number, "event_time_utc": _iso(et), "ingest_time_utc": _iso(m.ingest_time),
            "lateness_s": round(lateness, 3), "is_late": lateness > LATE_S, "tenant_id": tenant, "agent_id": m.agent_id,
            "release_version": version, "agent_sk": sk, "sandbox_instance_id": m.sandbox_instance_id,
            "status": m.status, "latency_ms": m.latency_ms, "model_id": m.model_id, "content_source": m.content_source,
            "stated_goal_fingerprint": rec.get("stated_goal_fingerprint"),
            "injection_indicators": m.injection_indicators if m.event_type == "llm.turn" else None,
            "tokens_in": m.input_tokens if m.event_type == "llm.turn" else None,
            "tokens_out": m.output_tokens if m.event_type == "llm.turn" else None,
            "finish_reason": m.finish_reason, "tool_id": tool_id, "tool_version": m.tool_version,
            "finished_at_utc": _iso(m.finished_at), "retry_number": m.retry_number, "error_class": m.error_class,
            "target_resource": m.target_resource, "bytes_out": m.bytes_out, "task_id": task.get("task_id"),
            "task_category": task.get("task_category"),
            "expected_tool_categories": task.get("expected_tool_categories"),
            "quality_flags": "|".join(flags), "source_file": msg.source_file, "source_line": msg.source_line,
            "kafka_partition": msg.partition, "kafka_offset": msg.offset,
        }
        prev = self.max_by_partition.get(msg.partition)
        if prev is None or et > prev:
            self.max_by_partition[msg.partition] = et
        self.stats["accepted"] += 1
        return Outcome(msg, "accepted", record=record, event_id=m.event_id, session_id=m.session_id)


# =============================================================================== sink

class Sink:
    """Writes outcomes to Kafka (curated / rejected topics) and PostgreSQL, idempotently."""

    def __init__(self, producer, conn, schema: str = config.DW_SCHEMA):
        self.producer, self.conn, self.schema = producer, conn, schema
        with conn.cursor() as cur:
            cur.execute(f"SELECT source_file_id FROM {schema}.dim_source_file WHERE file_name = 'stream_events.json' "
                        f"ORDER BY ingested_at DESC LIMIT 1")
            row = cur.fetchone()
        if row is None:
            raise SystemExit("dim_source_file has no stream_events.json: run the warehouse build first")
        self.source_file_id = row[0]

    def write(self, outcomes: list[Outcome]) -> None:
        if not outcomes:
            return
        from psycopg2.extras import execute_values
        futures = []
        for o in outcomes:
            key = o.session_id.encode() if o.session_id else o.msg.key
            if o.status == "accepted":
                futures.append(self.producer.send(config.TOPIC_CURATED, key=key, value=o.record,
                                                  partition=o.msg.partition))
            elif o.status == "rejected":
                futures.append(self.producer.send(config.TOPIC_REJECTED, key=key, value=self._reject_value(o),
                                                  partition=o.msg.partition))
            elif o.detail == "EOS":                              # forward the end marker to the curated readers
                futures.append(self.producer.send(config.TOPIC_CURATED, key=b"EOS", value=json.loads(o.msg.value),
                                                  partition=o.msg.partition))
        self.producer.flush()
        for f in futures:
            f.get(timeout=30)                                    # raises if any write failed
        acc = [o.record for o in outcomes if o.status == "accepted"]
        rej = [self._reject_row(o) for o in outcomes if o.status == "rejected"]
        log = [(o.msg.topic, o.msg.partition, o.msg.offset, o.event_id, o.session_id, o.msg.source_line, o.status,
                o.reason) for o in outcomes]
        s = self.schema
        with self.conn, self.conn.cursor() as cur:
            if acc:
                cols = CURATED_FIELDS
                sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "event_id")
                execute_values(cur, f"INSERT INTO {s}.stream_curated_event ({', '.join(cols)}) VALUES %s "
                                    f"ON CONFLICT (event_id) DO UPDATE SET {sets}",
                               [tuple(r[c] for c in cols) for r in acc])
            if rej:
                cols = ["source_file", "source_line", "source_file_id", "source_format", "record_id", "reason",
                        "reason_detail", "raw_payload", "zone", "channel", "detected_by", "kafka_topic",
                        "kafka_partition", "kafka_offset"]
                sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[2:])
                execute_values(cur, f"INSERT INTO {s}.fact_rejected_record ({', '.join(cols)}) VALUES %s "
                                    f"ON CONFLICT (source_file, source_line) DO UPDATE SET {sets}", rej)
            execute_values(cur, f"INSERT INTO {s}.stream_message_log (kafka_topic, kafka_partition, kafka_offset, "
                                f"event_id, session_id, source_line, outcome, reason) VALUES %s "
                                f"ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO UPDATE SET "
                                f"event_id = EXCLUDED.event_id, outcome = EXCLUDED.outcome, reason = EXCLUDED.reason",
                           log)

    def _reject_value(self, o: Outcome) -> dict:
        return {"source_file": o.msg.source_file, "source_line": o.msg.source_line, "record_id": o.event_id,
                "reason": o.reason, "reason_detail": o.detail, "raw_payload": o.msg.value.decode("utf-8", "replace"),
                "kafka_topic": o.msg.topic, "kafka_partition": o.msg.partition, "kafka_offset": o.msg.offset}

    def _reject_row(self, o: Outcome) -> tuple:
        return (o.msg.source_file, o.msg.source_line, self.source_file_id, "json_array", o.event_id, o.reason,
                o.detail, o.msg.value.decode("utf-8", "replace"), "stream", "stream", "stream_consumer",
                o.msg.topic, o.msg.partition, o.msg.offset)


def restore_state(judge: StreamJudge, conn, schema: str = config.DW_SCHEMA) -> int:
    """Rebuild the judge's state from what was already committed to the database."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT kafka_partition, kafka_offset, event_id FROM {schema}.stream_message_log "
                    f"WHERE kafka_topic = %s", (config.TOPIC_RAW,))
        rows = cur.fetchall()
        for p, o, eid in rows:
            judge.done.add((p, o))
            if eid:
                judge.seen.setdefault(eid, (p, o))
        cur.execute(f"SELECT session_id, task_id, task_category, expected_tool_categories FROM "
                    f"{schema}.stream_curated_event WHERE event_type = 'session.start'")
        for sid, t, c, e in cur.fetchall():
            judge.starts[sid] = {"task_id": t, "task_category": c, "expected_tool_categories": e}
        cur.execute(f"SELECT kafka_partition, max(event_time_utc) FROM {schema}.stream_curated_event GROUP BY 1")
        judge.max_by_partition.update(dict(cur.fetchall()))
        cur.execute(f"SELECT DISTINCT kafka_partition FROM {schema}.stream_message_log "
                    f"WHERE kafka_topic = %s AND outcome = 'control'", (config.TOPIC_RAW,))
        judge.eos_partitions.update(r[0] for r in cur.fetchall())
    return len(rows)


def reset_state(conn, schema: str = config.DW_SCHEMA) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {schema}.stream_curated_event, {schema}.stream_message_log, "
                    f"{schema}.stream_tenant_minute")
        cur.execute(f"DELETE FROM {schema}.fact_rejected_record WHERE channel = 'stream'")


# =============================================================================== main

def run(group: str, offset_reset: str, idle_timeout: float, reset: bool = False, max_poll: int = 500) -> dict:
    import psycopg2
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.structs import OffsetAndMetadata

    from .. import warehouse

    conn = psycopg2.connect(config.psycopg_dsn())
    if reset:
        reset_state(conn)
    judge = StreamJudge(Dims.from_db(conn))
    restored = restore_state(judge, conn)
    producer = KafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP, acks="all", linger_ms=5,
                             value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
    sink = Sink(producer, conn)
    consumer = KafkaConsumer(config.TOPIC_RAW, bootstrap_servers=config.KAFKA_BOOTSTRAP, group_id=group,
                             enable_auto_commit=False, auto_offset_reset=offset_reset,
                             max_poll_records=max_poll, consumer_timeout_ms=1000)
    t0 = time.time()
    first_offsets: dict[int, int] = {}
    next_offset: dict[int, int] = {}
    counts = Counter()
    last_msg = time.time()
    committed_batches = 0
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000, max_records=max_poll)
            msgs = [Msg.from_kafka(m) for recs in batch.values() for m in recs]
            outcomes: list[Outcome] = []
            for m in msgs:
                first_offsets.setdefault(m.partition, m.offset)
                next_offset[m.partition] = m.offset + 1
                outcomes += judge.process(m)
            if msgs:
                last_msg = time.time()
                counts["messages"] += len(msgs)
            assigned = {tp.partition for tp in consumer.assignment()}
            finished = bool(assigned) and assigned <= judge.eos_partitions
            if finished:
                outcomes += judge.end_of_stream()
            for o in outcomes:
                counts[o.status] += 1
            # write first (Kafka + database), then commit offsets: at-least-once
            sink.write(outcomes)
            if next_offset:
                held = judge.pending_min_offset()
                offsets = {TopicPartition(config.TOPIC_RAW, p): OffsetAndMetadata(min(o, held.get(p, o)), "", -1)
                           for p, o in next_offset.items()}
                consumer.commit(offsets)
                committed_batches += 1
            if finished:
                break
            if not msgs and time.time() - last_msg > idle_timeout:
                break
        committed = {}
        for p in sorted(consumer.assignment(), key=lambda tp: tp.partition):
            committed[str(p.partition)] = consumer.committed(p)
    finally:
        consumer.close()
        producer.close()
    with conn, conn.cursor() as cur:
        warehouse.refresh_aggregates(cur, config.DW_SCHEMA)
    conn.close()
    rejected = {k.split(":", 1)[1]: v for k, v in judge.stats.items() if k.startswith("rejected:")}
    return {
        "group_id": group, "auto_offset_reset": offset_reset, "restored_log_rows": restored,
        "eos_seen_on_partitions": sorted(judge.eos_partitions),
        "first_offset_read": {str(k): v for k, v in sorted(first_offsets.items())},
        "committed_offsets": committed, "messages_read": counts["messages"],
        "data_messages": counts["messages"] - counts["control"], "control_messages": counts["control"],
        "accepted": counts["accepted"], "rejected": counts["rejected"],
        "rejected_by_reason": dict(sorted(rejected.items(), key=lambda kv: RANK[kv[0]])),
        "held_for_session_start": judge.stats["held_for_session_start"],
        "released_after_start": judge.stats["released_after_start"],
        "orphan_by_watermark": judge.stats["orphan_by_watermark"], "orphan_at_eos": judge.stats["orphan_at_eos"],
        "redelivered_skipped": judge.stats["redelivered_skipped"], "commits": committed_batches,
        "elapsed_s": round(time.time() - t0, 1), "watermark_example": judge.watermark_example,
    }


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Kestrel stream consumer")
    p.add_argument("--group", default=config.CONSUMER_GROUP)
    p.add_argument("--offset-reset", default="earliest", choices=["earliest", "latest"])
    p.add_argument("--idle-timeout", type=float, default=60.0, help="stop after this many idle seconds without EOS")
    p.add_argument("--reset-state", action="store_true", help="truncate the stream tables first (after a topic reset)")
    p.add_argument("--summary", default="consumer_summary.json")
    args = p.parse_args(argv)
    summary = run(args.group, args.offset_reset, args.idle_timeout, args.reset_state)
    out = config.OUT_DIR / "stream"
    out.mkdir(parents=True, exist_ok=True)
    (out / args.summary).write_text(json.dumps(summary, indent=2, default=str))
    if summary["watermark_example"]:
        (out / "watermark_caught_example.json").write_text(json.dumps(summary["watermark_example"], indent=2))
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
