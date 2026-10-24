import subprocess
import sys

from filelock import FileLock

from retail.engine import Engine
from retail.generator import batches, encode
from retail.query import report


def test_readers_do_not_wait_for_writer_lock(tmp_path):
    engine = Engine(tmp_path)
    engine.ingest(encode(batches(1)[0]))
    with FileLock(str(tmp_path / "writer.lock"), timeout=0):
        assert report(tmp_path)["reconciled"]


def test_abrupt_process_exit_between_iceberg_commits(tmp_path):
    first, second, _ = batches(10)
    engine = Engine(tmp_path)
    engine.ingest(encode(first))
    original = report(tmp_path)
    source = tmp_path / "batch.jsonl"
    source.write_bytes(encode(second))
    child = """
import os, sys
from retail.engine import Engine
from retail.storage import Store
original = Store.write
def exit_between_commits(self, name, rows, evolved):
    result = original(self, name, rows, evolved)
    if name == "current":
        os._exit(77)
    return result
Store.write = exit_between_commits
Engine(sys.argv[1]).ingest_file(sys.argv[2])
"""
    result = subprocess.run([sys.executable, "-c", child, str(tmp_path), str(source)], timeout=45)
    assert result.returncode == 77
    assert report(tmp_path) == original
    engine.ingest_file(source)
    assert report(tmp_path)["reconciled"]
    with engine.store.control() as db:
        assert (
            db.execute("SELECT count(*) FROM attempts WHERE status='abandoned'").fetchone()[0] == 1
        )
