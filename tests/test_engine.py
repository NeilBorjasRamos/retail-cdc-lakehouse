import hashlib
from copy import deepcopy

import pytest
from filelock import FileLock, Timeout

from retail.cli import main, run_demo
from retail.contract import ContractError, canonical, normalize
from retail.engine import Engine
from retail.generator import batches, encode
from retail.model import materialize
from retail.query import report


def test_full_demo_known_totals(tmp_path):
    result = run_demo(tmp_path, 1000)
    assert result["counts"] == {"changes": 2467, "current": 800, "history": 2267, "gold": 4}
    assert result["totals"] == [
        {"currency": "PEN", "orders": 400, "amount_minor": 3_800_000},
        {"currency": "USD", "orders": 400, "amount_minor": 3_800_000},
    ]
    assert result["validation"]["historical_publication_unchanged"]


@pytest.mark.parametrize("point", ["changes", "current", "history", "gold", "validation"])
def test_failure_does_not_publish_and_retry_recovers(tmp_path, point):
    engine = Engine(tmp_path)
    first, second, _ = batches(10)
    engine.ingest(encode(first))
    original = report(tmp_path)
    with pytest.raises(RuntimeError, match="Injected"):
        engine.ingest(encode(second), fail_after=point)
    assert report(tmp_path) == original
    assert engine.store.publication()["next_offsets"] == {"retail.public.orders:0": 10}
    engine.ingest(encode(second))
    assert report(tmp_path)["reconciled"]
    assert report(tmp_path, 1) == original
    with engine.store.control() as db:
        assert db.execute("SELECT count(*) FROM attempts WHERE status='failed'").fetchone()[0] == 1


def test_replay_and_same_coordinate_conflict(tmp_path):
    engine = Engine(tmp_path)
    records = batches(3)[0]
    data = encode(records)
    engine.ingest(data)
    assert engine.ingest(data)["added"] == 0
    records[0]["value"]["after"]["amount_minor"] += 1
    with pytest.raises(ContractError, match="coordinate"):
        engine.ingest(encode(records))
    assert report(tmp_path)["generation"] == 1


def test_semantic_duplicate_at_new_offset_and_conflict(tmp_path):
    engine = Engine(tmp_path)
    record = batches(1)[0][0]
    engine.ingest(encode([record]))
    record["offset"] = 1
    engine.ingest(encode([record]))
    assert report(tmp_path)["counts"]["history"] == 1
    record["offset"] = 2
    record["value"]["after"]["amount_minor"] += 1
    with pytest.raises(ContractError, match="revision"):
        engine.ingest(encode([record]))


def test_schema_evolution_and_late_revision(tmp_path):
    engine = Engine(tmp_path)
    first, second, third = batches(3)
    engine.ingest(encode(first))
    current = engine.store.catalog.load_table("retail.current")
    assert "region" not in current.schema().column_names
    engine.ingest(encode(second))
    assert "region" in engine.store.catalog.load_table("retail.current").schema().column_names
    before = engine.store.read("current", engine.store.publication()).to_pylist()
    engine.ingest(encode(third))
    assert engine.store.read("current", engine.store.publication()).to_pylist() == before
    history = engine.store.read("history", engine.store.publication()).to_pylist()
    versions = [row for row in history if row["order_id"] == "O00000003"]
    assert [(r["revision"], r["valid_to_revision"]) for r in versions] == [
        (1, 2),
        (2, 3),
        (3, None),
    ]
    assert report(tmp_path, 1)["counts"]["current"] == 3


def test_delete_all_and_tombstone_only(tmp_path):
    engine = Engine(tmp_path)
    record = batches(1)[0][0]
    record["value"] = None
    engine.ingest(encode([record]))
    assert report(tmp_path)["counts"]["current"] == 0
    state = batches(1)[0][0]["value"]["after"]
    record["offset"] = 1
    record["value"] = {
        "before": state,
        "after": None,
        "op": "d",
        "source": {"db": "retail", "table": "orders"},
    }
    engine.ingest(encode([record]))
    assert report(tmp_path)["reconciled"]
    assert report(tmp_path)["totals"] == []


def test_malformed_batch_preserved_without_advancing(tmp_path):
    engine = Engine(tmp_path)
    data = b"{bad}\n"
    with pytest.raises(ContractError):
        engine.ingest(data)
    assert (tmp_path / "bronze" / f"{hashlib.sha256(data).hexdigest()}.jsonl").read_bytes() == data
    assert engine.store.publication() is None
    assert report(tmp_path)["generation"] == 0


def test_out_of_order_transport_and_key_migration(tmp_path):
    engine = Engine(tmp_path)
    records = batches(3)[0]
    with pytest.raises(ContractError, match="offset"):
        engine.ingest(encode(list(reversed(records))))
    engine.ingest(encode(records))
    record = deepcopy(records[0])
    record["partition"] = 1
    with pytest.raises(ContractError, match="partition"):
        engine.ingest(encode([record]))


def test_known_replay_can_precede_new_records(tmp_path):
    engine = Engine(tmp_path)
    records = batches(3)[0]
    engine.ingest(encode(records[:1]))
    assert engine.ingest(encode(records))["added"] == 2


def test_bronze_corruption_is_detected(tmp_path):
    engine = Engine(tmp_path)
    data = encode(batches(1)[0])
    engine.ingest(data)
    path = tmp_path / "bronze" / f"{hashlib.sha256(data).hexdigest()}.jsonl"
    path.write_bytes(b"corrupted")
    with pytest.raises(RuntimeError, match="integrity"):
        engine.ingest(data)


def test_lock_and_publication_compare_and_swap(tmp_path):
    engine = Engine(tmp_path)
    with FileLock(str(tmp_path / "writer.lock"), timeout=0):
        with pytest.raises(Timeout):
            engine.ingest(encode(batches(1)[0]))
    engine.ingest(encode(batches(1)[0]))
    manifest = engine.store.publication()
    with pytest.raises(RuntimeError, match="compare-and-swap"):
        engine.store.publish(manifest, 0, 1)


def test_future_model_blocked(tmp_path):
    engine = Engine(tmp_path)
    engine.ingest(encode(batches(1)[0]))
    manifest = engine.store.publication()
    manifest["model_version"] = 99
    with engine.store.control() as db:
        db.execute("UPDATE publications SET manifest=?", (canonical(manifest),))
    with pytest.raises(ContractError, match="Model version"):
        engine.ingest(encode(batches(1)[1]))


def test_pure_model_is_independent_of_arrival_order():
    first, second, third = batches(9)
    events = [normalize(r) for r in first + second + third]
    forward = materialize(events)
    reverse = materialize(list(reversed(events)))
    assert forward["current"] == reverse["current"]
    assert forward["history"] == reverse["history"]


def test_cli_errors_and_report(tmp_path, capsys):
    assert main(["--root", str(tmp_path), "report"]) == 0
    assert '"generation": 0' in capsys.readouterr().out
    assert main(["--root", str(tmp_path), "ingest", str(tmp_path / "missing")]) == 1
    assert "missing" not in capsys.readouterr().out
    assert main(["--root", str(tmp_path), "report", "--generation", "42"]) == 1
