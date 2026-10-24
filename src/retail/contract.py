"""A deliberately bounded, schema-less Debezium PostgreSQL order-table adapter."""

import hashlib
import json
import re
from datetime import UTC, datetime

MAX_RECORDS = 100_000
MAX_INPUT_BYTES = 32 * 1024 * 1024
MAX_RECORD_BYTES = 64 * 1024


class ContractError(ValueError):
    """Input cannot be applied without risking silent data loss."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ContractError("Duplicate JSON key")
        result[key] = value
    return result


def loads(raw):
    try:
        return json.loads(raw, object_pairs_hook=unique_keys, parse_constant=reject_constant)
    except (ValueError, RecursionError, TypeError) as exc:
        raise ContractError("Invalid JSON") from exc


def reject_constant(_):
    raise ContractError("Non-finite JSON number")


def integer(value, minimum=0, maximum=2**63 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ContractError("Invalid integer")
    return value


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or not 2000 <= parsed.year <= 2099:
            raise ValueError()
        return parsed.astimezone(UTC).isoformat()
    except (TypeError, AttributeError, ValueError) as exc:
        raise ContractError("Expected an aware ISO timestamp in 2000..2099") from exc


def row(value):
    required = {
        "order_id",
        "customer_id",
        "revision",
        "amount_minor",
        "currency",
        "status",
        "updated_at",
    }
    if not isinstance(value, dict) or set(value) not in (required, required | {"region"}):
        raise ContractError("Unsupported row schema")
    if not isinstance(value["order_id"], str) or not re.fullmatch(r"O[0-9]{8}", value["order_id"]):
        raise ContractError("Invalid order key")
    if not isinstance(value["customer_id"], str) or not re.fullmatch(
        r"C[0-9]{6}", value["customer_id"]
    ):
        raise ContractError("Invalid customer key")
    if value["status"] not in ("created", "paid", "shipped", "cancelled"):
        raise ContractError("Unsupported status")
    if value["currency"] not in ("PEN", "USD"):
        raise ContractError("Unsupported currency")
    if value.get("region") is not None and value["region"] not in ("LIM", "ARE", "CUS"):
        raise ContractError("Unsupported region")
    return {
        **value,
        "revision": integer(value["revision"], 1, 1_000_000_000),
        "amount_minor": integer(value["amount_minor"], 1, 10_000_000_000),
        "updated_at": timestamp(value["updated_at"]),
    }


def normalize(record):
    if not isinstance(record, dict) or set(record) != {
        "topic",
        "partition",
        "offset",
        "key",
        "value",
    }:
        raise ContractError("Expected topic, partition, offset, key, value")
    topic = record["topic"]
    if not isinstance(topic, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,200}", topic):
        raise ContractError("Invalid topic")
    partition = integer(record["partition"], 0, 10_000)
    offset = integer(record["offset"])
    key = record["key"]
    if not isinstance(key, dict) or set(key) != {"order_id"}:
        raise ContractError("Expected an order_id Kafka key")
    if not isinstance(key["order_id"], str) or not re.fullmatch(r"O[0-9]{8}", key["order_id"]):
        raise ContractError("Invalid Kafka key")
    value = record["value"]
    result = {
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "raw_hash": digest(record),
        "raw": canonical(record),
        "key": key["order_id"],
    }
    if value is None:
        return {**result, "state": None, "evolve": False}
    if not isinstance(value, dict) or not {"before", "after", "op", "source"} <= set(value):
        raise ContractError("Expected a schema-less Debezium envelope")
    source = value["source"]
    if (
        not isinstance(source, dict)
        or source.get("table") != "orders"
        or source.get("db") != "retail"
    ):
        raise ContractError("Unexpected source; do not mix databases or tables")
    op = value["op"]
    if op in ("c", "r", "u"):
        if op in ("c", "r") and value["before"] is not None:
            raise ContractError("Create/snapshot must have null before")
        state = row(value["after"])
        if op == "u":
            before = row(value["before"])
            if before["order_id"] != state["order_id"] or before["revision"] >= state["revision"]:
                raise ContractError("Update must increase revision and preserve key")
        state["deleted"] = False
    elif op == "d" and value["after"] is None:
        state = row(value["before"])
        state["revision"] = integer(state["revision"] + 1, 1, 1_000_000_000)
        state["deleted"] = True
    else:
        raise ContractError("Unsupported CDC operation")
    if state["order_id"] != key["order_id"]:
        raise ContractError("Key and payload disagree")
    return {**result, "state": state, "evolve": "region" in state}


def parse_batch(data):
    if len(data) > MAX_INPUT_BYTES:
        raise ContractError("Batch exceeds 32 MiB")
    try:
        lines = data.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise ContractError("Input must be UTF-8") from exc
    if not lines or len(lines) > MAX_RECORDS:
        raise ContractError("Expected 1..100000 JSONL records")
    result = []
    for line in lines:
        if not line or len(line.encode()) > MAX_RECORD_BYTES:
            raise ContractError("Empty or oversized record")
        result.append(normalize(loads(line)))
    return result
