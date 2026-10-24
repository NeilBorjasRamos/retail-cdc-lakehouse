"""Readers capture one publication and use its four immutable snapshot IDs."""

from pathlib import Path

import duckdb

from retail import MODEL_VERSION
from retail.contract import canonical, loads, normalize
from retail.model import materialize
from retail.storage import TABLES, Store


def report(root, generation=None):
    if not (Path(root) / "control.sqlite").exists():
        if generation is not None:
            raise ValueError("Unknown generation")
        return {"generation": 0, "reconciled": True, "counts": {}, "totals": []}
    store = Store(root, initialize=False)
    publication = store.publication(generation)
    if publication is None:
        return {"generation": 0, "reconciled": True, "counts": {}, "totals": []}
    if publication.get("model_version") != MODEL_VERSION:
        raise ValueError("Use the application release matching the publication model")
    tables = {name: store.read(name, publication) for name in TABLES}
    events = [normalize(loads(row["raw"])) for row in tables["changes"].to_pylist()]
    expected = materialize(events)
    differences = {}
    for name in TABLES:
        actual = tables[name].to_pylist()
        wanted = expected[name]
        # Old snapshots are projected through the latest compatible Iceberg schema.
        if name in ("current", "history"):
            actual = [{**row, "region": row.get("region")} for row in actual]
        differences[name] = len(set(map(canonical, actual)) ^ set(map(canonical, wanted)))
        if len(actual) != len(wanted):
            differences[name] += abs(len(actual) - len(wanted))
    with duckdb.connect() as db:
        db.register("orders", tables["current"])
        totals = db.execute(
            "SELECT currency, count(*) AS orders, "
            "sum(amount_minor)::BIGINT AS amount_minor FROM orders "
            "GROUP BY currency ORDER BY currency"
        ).fetchall()
    gaps = sum(
        row["valid_to_revision"] - row["revision"] - 1
        for row in expected["history"]
        if row["valid_to_revision"] is not None
    )
    return {
        "generation": publication["generation"],
        "reconciled": not any(differences.values()),
        "counts": {name: table.num_rows for name, table in tables.items()},
        "differences": differences,
        "snapshots": publication["snapshots"],
        "next_offsets": publication["next_offsets"],
        "revision_gaps": gaps,
        "totals": [{"currency": c, "orders": n, "amount_minor": a} for c, n, a in totals],
    }
