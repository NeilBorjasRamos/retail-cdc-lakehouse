"""Bounded single-partition ingestion using the published cursor as authority."""

import uuid

from confluent_kafka import Consumer, KafkaError, TopicPartition

from retail.contract import ContractError, loads
from retail.engine import Engine
from retail.generator import encode


def ingest_partition(root, bootstrap, topic, partition=0, limit=1000, client=None):
    if not 1 <= limit <= 10_000 or partition < 0:
        raise ValueError("Expected limit 1..10000 and nonnegative partition")
    engine = Engine(root)
    # Hold the same writer lock across cursor selection, poll and publish. The lock
    # is recursive within this instance; another worker fails fast, not a rebalance.
    with engine.store.lock:
        publication = engine.store.publication()
        offsets = publication["next_offsets"] if publication else {}
        start = offsets.get(f"{topic}:{partition}", 0)
        consumer = client or Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"retail-bounded-{uuid.uuid4()}",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "enable.partition.eof": True,
                "allow.auto.create.topics": False,
            }
        )
        try:
            tp = TopicPartition(topic, partition)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            if start < low or start > high:
                raise ContractError("Retention gap or topic reset; rebuild from a verified backup")
            consumer.assign([TopicPartition(topic, partition, start)])
            records = []
            while len(records) < limit:
                msg = consumer.poll(5)
                if msg is None:
                    raise TimeoutError("Kafka poll timed out; no cursor was advanced")
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    raise RuntimeError("Kafka record error")
                if msg.offset() >= high:
                    break  # bounded by the high watermark captured before polling
                records.append(
                    {
                        "topic": topic,
                        "partition": partition,
                        "offset": msg.offset(),
                        "key": loads(msg.key()),
                        "value": loads(msg.value()) if msg.value() is not None else None,
                    }
                )
            if not records:
                return {"generation": publication["generation"] if publication else 0, "added": 0}
            return engine.ingest(encode(records))
        finally:
            consumer.close()
