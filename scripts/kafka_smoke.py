"""Real broker drill. Only fictional data; uses a unique new topic per execution."""

import json
import os
import uuid
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from retail.contract import ContractError, canonical
from retail.generator import batches
from retail.kafka import ingest_partition
from retail.query import report


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    root = Path(os.environ.get("SMOKE_ROOT", "data/kafka-smoke"))
    if root.exists() and any(root.iterdir()):
        raise ValueError("Choose a fresh SMOKE_ROOT")
    topic = f"retail-smoke-{uuid.uuid4().hex[:12]}"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(20)
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 15000,
        }
    )
    errors = []

    def deliver(error, _message):
        if error:
            errors.append(str(error))

    records = [r for batch in batches(1000) for r in batch]
    for record in records:
        producer.produce(
            topic,
            partition=0,
            key=canonical(record["key"]).encode(),
            value=canonical(record["value"]).encode(),
            on_delivery=deliver,
        )
        producer.poll(0)
    if producer.flush(20) or errors:
        raise RuntimeError("Producer delivery failed")
    while True:
        result = ingest_partition(root, bootstrap, topic, limit=400)
        if not result["added"]:
            break
    result = report(root)
    assert result["reconciled"]
    assert result["counts"] == {"changes": 2467, "current": 800, "history": 2267, "gold": 4}
    assert result["totals"] == [
        {"currency": "PEN", "orders": 400, "amount_minor": 3_800_000},
        {"currency": "USD", "orders": 400, "amount_minor": 3_800_000},
    ]
    previous_generation = result["generation"]
    producer.produce(
        topic,
        partition=0,
        key=canonical(records[0]["key"]).encode(),
        value=b'{"unexpected":"contract-break"}',
        on_delivery=deliver,
    )
    if producer.flush(20) or errors:
        raise RuntimeError("Poison record delivery failed")
    try:
        ingest_partition(root, bootstrap, topic)
    except ContractError:
        pass
    else:
        raise AssertionError("Poison payload was silently accepted")
    assert report(root)["generation"] == previous_generation
    assert report(root)["next_offsets"] == {f"{topic}:0": 2467}
    result["validation"] = {
        "real_kafka": True,
        "synthetic": True,
        "contract_break_blocked_without_cursor_advance": True,
        "transport_records": len(records),
        "broker_version_expected": "3.9.1",
    }
    (root / "validation.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    print(f"Disposable smoke topic retained for inspection: {topic}")


if __name__ == "__main__":
    main()
