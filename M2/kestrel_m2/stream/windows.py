"""Bonus: tumbling one-minute windows per tenant, computed from the curated stream.

    python -m kestrel_m2.stream.windows

Reads ``kestrel.agent.events.curated`` (group ``kestrel-tata-windows``) and counts, per
tenant and per one-minute window of **event_time** ``[start, start + 60 s)``:
events, tool calls and denied tool calls.

Late data:

* The curated topic only holds records the consumer accepted, so nothing in it is more
  than 60 s behind the consumer's watermark: anything later was already rejected as
  ``LATE_BEYOND_WATERMARK`` and sits in ``fact_rejected_record``.
* This job keeps its own event-time watermark = min over partitions of the max
  event_time seen in that partition, minus 60 s (a partition that is behind holds the
  watermark back, so a fast partition cannot close windows early).
* A window stays **open** until the watermark passes its end, i.e. for 60 s of event time
  after the window closes; records arriving in that time update it. Then it is emitted
  once with ``is_final = true`` (append semantics).
* A record that still arrives for a window that is already final is not dropped: the
  window is corrected (upsert) and its ``late_updates`` counter goes up, so the
  correction is visible. At ``EOS`` every open window is finalised.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from .. import config


def window_start(ts: datetime, size_s: int = config.WINDOW_S) -> datetime:
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % size_s, tz=timezone.utc)


@dataclass
class TenantWindows:
    size_s: int = config.WINDOW_S
    lateness_s: int = config.WATERMARK_S
    partitions: int = config.TOPIC_PARTITIONS
    counts: dict = field(default_factory=lambda: defaultdict(lambda: {"events": 0, "tool_calls": 0,
                                                                      "denied_tool_calls": 0, "late_updates": 0}))
    final: set = field(default_factory=set)
    max_by_partition: dict = field(default_factory=dict)
    late_examples: list = field(default_factory=list)

    @property
    def watermark(self) -> Optional[datetime]:
        if len(self.max_by_partition) < self.partitions:
            return None
        return min(self.max_by_partition.values()) - timedelta(seconds=self.lateness_s)

    def add(self, rec: dict, partition: int) -> None:
        ts = datetime.fromisoformat(rec["event_time_utc"].replace("Z", "+00:00"))
        key = (rec["tenant_id"], window_start(ts, self.size_s))
        c = self.counts[key]
        c["events"] += 1
        if rec["event_type"] == "tool.call":
            c["tool_calls"] += 1
            c["denied_tool_calls"] += rec.get("status") == "denied"
        if key in self.final:                                   # correction of an emitted window
            c["late_updates"] += 1
            c["_dirty"] = True
            if len(self.late_examples) < 5:
                self.late_examples.append({"event_id": rec["event_id"], "tenant_id": key[0],
                                           "window_start": key[1].isoformat(), "event_time": rec["event_time_utc"]})
        prev = self.max_by_partition.get(partition)
        if prev is None or ts > prev:
            self.max_by_partition[partition] = ts

    def close_ready(self, everything: bool = False) -> list[tuple]:
        """Windows whose end the watermark has passed (or all, at end of stream)."""
        wm = self.watermark
        ready = []
        for key, c in self.counts.items():
            end = key[1] + timedelta(seconds=self.size_s)
            if key not in self.final and (everything or (wm is not None and wm >= end)) or \
                    (key in self.final and c.get("_dirty")):
                ready.append(key)
        rows = []
        for key in ready:
            self.final.add(key)
            c = self.counts[key]
            c.pop("_dirty", None)
            rows.append((key[0], key[1], key[1] + timedelta(seconds=self.size_s), c["events"], c["tool_calls"],
                         c["denied_tool_calls"], True, c["late_updates"]))
        return rows


def run(group: str, idle_timeout: float) -> dict:
    import psycopg2
    from kafka import KafkaConsumer
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(config.psycopg_dsn())
    consumer = KafkaConsumer(config.TOPIC_CURATED, bootstrap_servers=config.KAFKA_BOOTSTRAP, group_id=group,
                             enable_auto_commit=False, auto_offset_reset="earliest", max_poll_records=1000)
    w = TenantWindows()
    eos: set = set()
    emitted = 0
    records = 0
    last = time.time()
    t0 = time.time()
    s = config.DW_SCHEMA
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)
            msgs = [m for recs in batch.values() for m in recs]
            for m in msgs:
                rec = json.loads(m.value)
                if rec.get("event_type") == "EOS":
                    eos.add(m.partition)
                    continue
                w.add(rec, m.partition)
                records += 1
            if msgs:
                last = time.time()
            assigned = {tp.partition for tp in consumer.assignment()}
            done = bool(assigned) and assigned <= eos
            rows = w.close_ready(everything=done)
            if rows:
                with conn, conn.cursor() as cur:
                    execute_values(cur, f"INSERT INTO {s}.stream_tenant_minute (tenant_id, window_start, window_end, "
                                        f"events, tool_calls, denied_tool_calls, is_final, late_updates) VALUES %s "
                                        f"ON CONFLICT (tenant_id, window_start) DO UPDATE SET events = EXCLUDED.events, "
                                        f"tool_calls = EXCLUDED.tool_calls, "
                                        f"denied_tool_calls = EXCLUDED.denied_tool_calls, "
                                        f"is_final = EXCLUDED.is_final, late_updates = EXCLUDED.late_updates", rows)
                emitted += len(rows)
            if msgs:
                consumer.commit()                                     # after the window rows were written
            if done or (not msgs and time.time() - last > idle_timeout):
                break
    finally:
        consumer.close()
    with conn.cursor() as cur:
        cur.execute(f"SELECT tenant_id, window_start, window_end, events, tool_calls, denied_tool_calls, is_final, "
                    f"late_updates FROM {s}.stream_tenant_minute ORDER BY window_start, tenant_id")
        table = cur.fetchall()
    conn.close()
    out = config.OUT_DIR / "stream"
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "tenant_minute_windows.csv", "w", encoding="utf-8") as fh:
        fh.write("tenant_id,window_start,window_end,events,tool_calls,denied_tool_calls,is_final,late_updates\n")
        for r in table:
            fh.write(",".join(x.isoformat() if isinstance(x, datetime) else str(x) for x in r) + "\n")
    return {"records": records, "windows": len(table), "rows_written": emitted, "eos_partitions": sorted(eos),
            "late_updates": sum(c["late_updates"] for c in w.counts.values()), "late_examples": w.late_examples,
            "denied_tool_calls": sum(r[5] for r in table), "elapsed_s": round(time.time() - t0, 1)}


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="tumbling one-minute windows per tenant")
    p.add_argument("--group", default=config.WINDOW_GROUP)
    p.add_argument("--idle-timeout", type=float, default=30.0)
    args = p.parse_args(argv)
    summary = run(args.group, args.idle_timeout)
    out = config.OUT_DIR / "stream"
    (out / "windows_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
