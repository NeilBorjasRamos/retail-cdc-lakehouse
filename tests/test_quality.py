from copy import deepcopy

import pytest

from retail.contract import ContractError
from retail.engine import Engine
from retail.generator import batches, encode
from retail.query import report


def test_gap_diagnostic_closes_when_late_revision_arrives(tmp_path):
    engine = Engine(tmp_path)
    first, second, third = batches(3)
    engine.ingest(encode(first))
    engine.ingest(encode(second))
    assert report(tmp_path)["revision_gaps"] == 1
    engine.ingest(encode(third))
    assert report(tmp_path)["revision_gaps"] == 0


def test_id_reuse_after_delete_blocked(tmp_path):
    engine = Engine(tmp_path)
    first, second, _ = batches(5)
    engine.ingest(encode(first + second))
    record = deepcopy(first[-1])
    record["offset"] = 100
    record["value"]["after"]["revision"] = 3
    with pytest.raises(ContractError, match="reuse"):
        engine.ingest(encode([record]))


def test_persisted_candidate_validation_prevents_promotion(tmp_path, monkeypatch):
    engine = Engine(tmp_path)
    engine.ingest(encode(batches(3)[0]))
    original = report(tmp_path)
    write = engine.store.write

    def corrupt(name, rows, evolved):
        if name == "gold":
            rows = [{**row, "amount_minor": row["amount_minor"] + 1} for row in rows]
        return write(name, rows, evolved)

    monkeypatch.setattr(engine.store, "write", corrupt)
    with pytest.raises(RuntimeError, match="read-back"):
        engine.ingest(encode(batches(3)[1]))
    assert report(tmp_path) == original
