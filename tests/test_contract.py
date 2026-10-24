from copy import deepcopy

import pytest

from retail.contract import ContractError, loads, normalize, parse_batch
from retail.generator import batches, encode


@pytest.fixture
def record():
    return deepcopy(batches(1)[0][0])


@pytest.mark.parametrize(
    "field,value",
    [
        ("revision", True),
        ("revision", 0),
        ("amount_minor", 0),
        ("amount_minor", 1.5),
        ("currency", "EUR"),
        ("status", "refunded"),
        ("order_id", "Oabc"),
        ("customer_id", "real@email.com"),
        ("updated_at", "2025-01-01"),
        ("updated_at", "1990-01-01T00:00:00Z"),
        ("region", "XXX"),
    ],
)
def test_reject_bad_row(record, field, value):
    record["value"]["after"][field] = value
    with pytest.raises(ContractError):
        normalize(record)


@pytest.mark.parametrize(
    "field,value",
    [
        ("partition", -1),
        ("partition", False),
        ("offset", -1),
        ("topic", "bad:topic"),
        ("key", {"order_id": "O00000002"}),
    ],
)
def test_reject_bad_transport(record, field, value):
    record[field] = value
    with pytest.raises(ContractError):
        normalize(record)


@pytest.mark.parametrize("raw", [b"", b"\n", b"[]", b'{"a":1,"a":2}', b"NaN", b"\xff", b"[" * 1500])
def test_bad_json(raw):
    with pytest.raises(ContractError):
        parse_batch(raw)


def test_limits_and_unknown_fields(record):
    with pytest.raises(ContractError):
        parse_batch(b"x" * (32 * 1024 * 1024 + 1))
    with pytest.raises(ContractError):
        parse_batch(b" " * 65537)
    record["value"]["after"]["email"] = "not-allowed"
    with pytest.raises(ContractError):
        normalize(record)


def test_timestamp_and_tombstone(record):
    record["value"]["after"]["updated_at"] = "2025-01-01T07:00:00-05:00"
    assert normalize(record)["state"]["updated_at"] == "2025-01-01T12:00:00+00:00"
    record["value"] = None
    assert normalize(record)["state"] is None
    assert parse_batch(encode([record]))[0]["key"] == "O00000001"


def test_unsupported_envelope(record):
    for change in ({"op": "t"}, {"source": {"db": "other", "table": "orders"}}, {"after": None}):
        candidate = deepcopy(record)
        candidate["value"].update(change)
        with pytest.raises(ContractError):
            normalize(candidate)
    with pytest.raises(ContractError):
        loads('{"x":Infinity}')
