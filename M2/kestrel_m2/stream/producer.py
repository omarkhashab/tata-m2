"""Lab 7 producer: replay ``stream_events.json`` into Kafka at a fixed pace.

    python -m kestrel_m2.stream.producer --rate 400          # messages per second
    python -m kestrel_m2.stream.producer --reset-topics      # delete + recreate the three topics first

Topology (Dataset Description, Appendix B):

* ``kestrel.agent.events`` (raw), 3 partitions, **key = session_id** so one session's
  messages stay in one partition and keep their relative order;
* ``kestrel.agent.events.curated`` and ``kestrel.agent.events.rejected``, written by
  the consumer, same partitioning.

One array element = one message, in file order (the file is ordered by ingest_time),
sent at a fixed rate. Each message carries headers ``source_file``, ``source_line``
(physical line where the element starts) and ``replay_seq``, so every downstream row
points back at its line in the file.

**No raw prompt or response text on the stream.** Only the operational fields of the
contract (``contracts.STREAM_FIELDS``) are forwarded. ``stated_goal`` (text the LLM
wrote) is replaced by ``stated_goal_fingerprint`` = first 16 hex of its SHA-256, which
still shows *that* a goal changed without carrying what it says. Unknown fields are
dropped, not forwarded. An element that is not a JSON object (the feed has truncated
records serialised as strings) is forwarded as the JSON string it is, after removing any
``"stated_goal": "..."`` fragment, so the consumer can reject it as UNPARSEABLE_RECORD.

The replay ends with the ``EOS`` control record, sent to **every** partition, so each
partition's reader knows the stream is complete.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from collections import Counter
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

from .. import config, contracts

TOPICS = [config.TOPIC_RAW, config.TOPIC_CURATED, config.TOPIC_REJECTED]
_GOAL_RX = re.compile(r'"stated_goal"\s*:\s*"(?:[^"\\]|\\.)*"?')
_SESSION_RX = re.compile(r'"session_id"\s*:\s*"(ss_\w+)"')


def ensure_topics(bootstrap: str, reset: bool = False, timeout_s: float = 90.0) -> None:
    """Create the three topics (3 partitions each); with ``reset`` delete them first.

    Topic deletion is asynchronous, so a reset waits until the topics are really gone,
    retries creation while the broker still reports them as existing, and returns only
    when every partition of every topic has a leader.
    """
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="kestrel-admin")
    deadline = time.monotonic() + timeout_s
    try:
        if reset:
            existing = [t for t in TOPICS if t in admin.list_topics()]
            if existing:
                try:
                    admin.delete_topics(existing)
                except UnknownTopicOrPartitionError:
                    pass
            while set(TOPICS) & set(admin.list_topics()):
                if time.monotonic() > deadline:
                    raise TimeoutError("topics still being deleted")
                time.sleep(0.5)
        missing = [t for t in TOPICS if t not in admin.list_topics()]
        while missing:
            for t in missing:
                try:
                    admin.create_topics([NewTopic(t, num_partitions=config.TOPIC_PARTITIONS, replication_factor=1)])
                except TopicAlreadyExistsError:
                    pass                                          # may still be marked for deletion: retry
            time.sleep(0.5)
            missing = [t for t in TOPICS if t not in admin.list_topics()]
            if missing and time.monotonic() > deadline:
                raise TimeoutError(f"could not create {missing}")
        while True:
            desc = admin.describe_topics(TOPICS)
            ready = all(len(d["partitions"]) == config.TOPIC_PARTITIONS and
                        all(p["leader"] is not None and p["leader"] >= 0 for p in d["partitions"]) for d in desc)
            if ready:
                return
            if time.monotonic() > deadline:
                raise TimeoutError("topic partitions have no leader")
            time.sleep(0.5)
    finally:
        admin.close()


def elements_with_lines(path: Path):
    """Yield (line, element) for each top-level array element, line = where it starts."""
    text = path.read_text(encoding="utf-8")
    dec = json.JSONDecoder()
    i = text.index("[") + 1
    line, pos = 1 + text.count("\n", 0, i), i
    n = len(text)
    while True:
        while i < n and text[i] in " \t\r\n,":
            i += 1
        if i >= n or text[i] == "]":
            return
        line += text.count("\n", pos, i)
        pos = i
        value, i = dec.raw_decode(text, i)
        yield line, value


def fingerprint(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def sanitise(element, stats: Counter):
    """The message value that may go on the stream (operational metadata + indicators only)."""
    if isinstance(element, dict):
        if element.get("event_type") == "EOS":
            return {k: element.get(k) for k in contracts.EOS_FIELDS}
        out = {k: element.get(k) for k in contracts.STREAM_FIELDS if k in element}
        goal = element.get("stated_goal")
        out["stated_goal_fingerprint"] = fingerprint(goal) if isinstance(goal, str) else None
        if isinstance(goal, str):
            stats["stated_goal_fingerprinted"] += 1
        for k in element:
            if k not in contracts.STREAM_FIELDS and k not in contracts.STREAM_TEXT_FIELDS:
                stats[f"dropped_field:{k}"] += 1
        return out
    if isinstance(element, str):
        cleaned, n = _GOAL_RX.subn('"stated_goal":"<removed>"', element)
        stats["non_object_elements"] += 1
        stats["text_fragments_removed"] += n
        return cleaned
    stats["other_elements"] += 1
    return element


def key_of(element, value) -> bytes | None:
    if isinstance(element, dict) and isinstance(element.get("session_id"), str):
        return element["session_id"].encode()
    if isinstance(element, str):                              # truncated record: recover the key if visible
        m = _SESSION_RX.search(element)
        return m.group(1).encode() if m else None
    return None


def run(path: Path, bootstrap: str, rate: float, reset: bool = False, producer_id: str = "replay-1") -> dict:
    ensure_topics(bootstrap, reset)
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=10, linger_ms=5,
                             max_in_flight_requests_per_connection=1,      # keeps per-partition order on retry
                             value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
    stats: Counter = Counter()
    per_partition: Counter = Counter()
    eos = None
    start = time.monotonic()
    sent = 0
    futures = []
    for line, element in elements_with_lines(path):
        if isinstance(element, dict) and element.get("event_type") == "EOS":
            eos = (line, element)                                # the terminator goes last, to every partition
            continue
        value = sanitise(element, stats)
        headers = [("source_file", path.name.encode()), ("source_line", str(line).encode()),
                   ("replay_seq", str(sent).encode()), ("producer_id", producer_id.encode())]
        delay = start + sent / rate - time.monotonic()           # fixed pace
        if delay > 0:
            time.sleep(delay)
        futures.append(producer.send(config.TOPIC_RAW, key=key_of(element, value), value=value, headers=headers))
        sent += 1
        if len(futures) >= 500:
            for f in futures:
                per_partition[f.get(timeout=30).partition] += 1
            futures = []
    for f in futures:
        per_partition[f.get(timeout=30).partition] += 1
    if eos is None:
        raise SystemExit("stream file has no EOS control record")
    line, element = eos
    for p in range(config.TOPIC_PARTITIONS):
        headers = [("source_file", path.name.encode()), ("source_line", str(line).encode()),
                   ("replay_seq", str(sent).encode()), ("producer_id", producer_id.encode())]
        producer.send(config.TOPIC_RAW, key=b"EOS", value=sanitise(element, stats), headers=headers,
                      partition=p).get(timeout=30)
    producer.flush()
    producer.close()
    elapsed = time.monotonic() - start
    summary = {"messages": sent, "eos_records": config.TOPIC_PARTITIONS, "rate_target_per_s": rate,
               "elapsed_s": round(elapsed, 1), "rate_achieved_per_s": round(sent / elapsed, 1),
               "per_partition": {str(k): v for k, v in sorted(per_partition.items())}, **dict(stats)}
    return summary


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="replay stream_events.json into Kafka")
    p.add_argument("--file", type=Path, default=config.RAW_DIR / "stream_events.json")
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    p.add_argument("--rate", type=float, default=400.0, help="messages per second (fixed pace)")
    p.add_argument("--reset-topics", action="store_true")
    args = p.parse_args(argv)
    summary = run(args.file, args.bootstrap, args.rate, args.reset_topics)
    out = config.OUT_DIR / "stream"
    out.mkdir(parents=True, exist_ok=True)
    (out / "producer_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
