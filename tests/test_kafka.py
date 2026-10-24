from copy import deepcopy

import pytest
from confluent_kafka import KafkaError

from retail.contract import ContractError, canonical
from retail.generator import batches
from retail.kafka import ingest_partition
from retail.query import report


class Message:
    def __init__(self, record=None, error=None):
        self.record, self.problem = record, error

    def error(self):
        return self.problem

    def offset(self):
        return self.record["offset"]

    def key(self):
        return canonical(self.record["key"]).encode()

    def value(self):
        value = self.record["value"]
        return canonical(value).encode() if value is not None else None


class Client:
    def __init__(self, messages, low=0, high=1):
        self.messages, self.low, self.high = iter(messages), low, high
        self.closed = False
        self.assignment = None

    def get_watermark_offsets(self, tp, timeout):
        return self.low, self.high

    def assign(self, partitions):
        self.assignment = partitions

    def poll(self, _):
        return next(self.messages, Message(error=KafkaError(KafkaError._PARTITION_EOF)))

    def close(self):
        self.closed = True


def test_cursor_authority_and_tombstone(tmp_path):
    record = batches(1)[0][0]
    client = Client([Message(record)])
    assert ingest_partition(tmp_path, "unused", record["topic"], client=client)["added"] == 1
    assert client.closed
    assert client.assignment[0].offset == 0
    record = deepcopy(record)
    record.update(offset=1, value=None)
    second = Client([Message(record)], high=2)
    ingest_partition(tmp_path, "unused", record["topic"], client=second)
    assert second.assignment[0].offset == 1
    assert report(tmp_path)["counts"]["current"] == 1


@pytest.mark.parametrize("low,high", [(1, 2), (0, -1)])
def test_retention_gap_and_reset_fail_closed(tmp_path, low, high):
    client = Client([], low=low, high=high)
    with pytest.raises(ContractError, match="Retention"):
        ingest_partition(tmp_path, "unused", "retail.public.orders", client=client)
    assert client.closed
    assert report(tmp_path)["generation"] == 0


@pytest.mark.parametrize(
    "message,error",
    [
        (None, TimeoutError),
        (Message(error=KafkaError(KafkaError._TRANSPORT)), RuntimeError),
    ],
)
def test_broker_failure_no_checkpoint(tmp_path, message, error):
    client = Client([message])
    with pytest.raises(error):
        ingest_partition(tmp_path, "unused", "retail.public.orders", client=client)
    assert client.closed
    assert report(tmp_path)["generation"] == 0


def test_empty_partition_and_bound(tmp_path):
    topic = "retail.public.orders"
    assert ingest_partition(tmp_path, "unused", topic, client=Client([], high=0))["added"] == 0
    record = batches(1)[0][0]
    record["offset"] = 2
    assert (
        ingest_partition(tmp_path, "unused", topic, client=Client([Message(record)], high=2))[
            "added"
        ]
        == 0
    )
    with pytest.raises(ValueError):
        ingest_partition(tmp_path, "unused", topic, limit=0)


def test_poison_payload_no_checkpoint(tmp_path):
    record = batches(1)[0][0]
    record["value"]["after"]["currency"] = "BAD"
    with pytest.raises(ContractError):
        ingest_partition(tmp_path, "unused", record["topic"], client=Client([Message(record)]))
    assert report(tmp_path)["generation"] == 0
