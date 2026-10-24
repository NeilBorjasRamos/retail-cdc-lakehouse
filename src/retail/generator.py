"""Fictional orders with updates, deletes, late revisions and schema evolution."""

from pathlib import Path

from retail.contract import canonical


def batches(count=1000):
    if not 1 <= count <= 10_000:
        raise ValueError("Generate 1..10000 orders")
    first, second, third = [], [], []
    offset = 0

    def event(before, after, op):
        nonlocal offset
        state = after or before
        value = {
            "before": before,
            "after": after,
            "op": op,
            "source": {"db": "retail", "table": "orders", "lsn": offset + 1000},
        }
        record = {
            "topic": "retail.public.orders",
            "partition": 0,
            "offset": offset,
            "key": {"order_id": state["order_id"]},
            "value": value,
        }
        offset += 1
        return record

    rows = []
    for i in range(1, count + 1):
        state = {
            "order_id": f"O{i:08d}",
            "customer_id": f"C{i % 100:06d}",
            "revision": 1,
            "amount_minor": 1000 + i * 17,
            "currency": "PEN" if i % 2 else "USD",
            "status": "created",
            "updated_at": "2025-01-01T12:00:00Z",
        }
        rows.append(state)
        first.append(event(None, state, "r"))
    for i, state in enumerate(rows, 1):
        paid = {**state, "revision": 2, "status": "paid", "updated_at": "2025-01-02T12:00:00Z"}
        if i % 5 == 0:
            second.append(event(state, None, "d"))
            tombstone = event(None, state, "r")
            tombstone["value"] = None
            second.append(tombstone)
        elif i % 3 == 0:
            # Arrive revision 3 before 2: source delivery order != logical version order.
            shipped = {
                **paid,
                "revision": 3,
                "status": "shipped",
                "region": "LIM",
                "updated_at": "2025-01-03T12:00:00Z",
            }
            second.append(event(paid, shipped, "u"))
            third.append((state, paid))
        else:
            second.append(event(state, paid, "u"))
    # Allocate all offsets in transport order, even for late business revisions.
    third = [event(before, after, "u") for before, after in third]
    return first, second, third


def encode(records):
    return ("\n".join(canonical(record) for record in records) + "\n").encode()


def write_batches(directory, count=1000):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for index, records in enumerate(batches(count), 1):
        if records:
            path = directory / f"batch-{index:02d}.jsonl"
            if path.exists():
                raise FileExistsError("Use a fresh input directory")
            path.write_bytes(encode(records))
            paths.append(str(path))
    return paths
