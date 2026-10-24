"""Replay-safe publication; failure never advances the durable consumer cursor."""

import hashlib
import os
import tempfile

from retail import MODEL_VERSION
from retail.contract import MAX_INPUT_BYTES, ContractError, canonical, loads, normalize, parse_batch
from retail.model import materialize
from retail.storage import TABLES, Store


def coordinate(event):
    return event["topic"], event["partition"], event["offset"]


def checkpoint_key(event):
    return f"{event['topic']}:{event['partition']}"


class Engine:
    def __init__(self, root):
        self.store = Store(root)

    def ingest_file(self, path, fail_after=None):
        with open(path, "rb") as source:
            data = source.read(MAX_INPUT_BYTES + 1)
        return self.ingest(data, fail_after=fail_after)

    def ingest(self, data, fail_after=None):
        if not isinstance(data, bytes) or len(data) > MAX_INPUT_BYTES:
            raise ContractError("Expected at most 32 MiB of bytes")
        with self.store.lock:
            sha = hashlib.sha256(data).hexdigest()
            bronze = self.store.root / "bronze"
            bronze.mkdir(exist_ok=True)
            path = bronze / f"{sha}.jsonl"
            if path.exists():
                if path.read_bytes() != data:
                    raise RuntimeError("Bronze integrity mismatch")
            else:
                # Publish a fully flushed file; crash leftovers have a .tmp suffix.
                fd, temporary = tempfile.mkstemp(dir=bronze, suffix=".tmp")
                try:
                    with os.fdopen(fd, "wb") as raw:
                        raw.write(data)
                        raw.flush()
                        os.fsync(raw.fileno())
                    os.replace(temporary, path)
                finally:
                    if os.path.exists(temporary):
                        os.unlink(temporary)
                directory = os.open(bronze, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            with self.store.control() as db:
                # The exclusive OS lock proves no previous writer is still active.
                db.execute("UPDATE attempts SET status='abandoned' WHERE status='started'")
                attempt = db.execute(
                    "INSERT INTO attempts(input_sha,status) VALUES (?,'started')", (sha,)
                ).lastrowid
            try:
                return self._apply(data, sha, attempt, fail_after)
            except Exception:
                with self.store.control() as db:
                    db.execute("UPDATE attempts SET status='failed' WHERE id=?", (attempt,))
                raise

    def _apply(self, data, sha, attempt, fail_after):
        incoming = parse_batch(data)
        previous = self.store.publication()
        if previous and previous.get("model_version") != MODEL_VERSION:
            raise ContractError("Model version changed; migrate to a new root")
        generation = previous["generation"] if previous else 0
        old = [
            normalize(loads(row["raw"])) for row in self.store.read("changes", previous).to_pylist()
        ]
        ledger = {coordinate(event): event for event in old}
        offsets = dict(previous["next_offsets"]) if previous else {}
        added = 0
        # A batch can contain late logical revisions, but transport offsets must be
        # nondecreasing per partition. Replaying known coordinates is permitted.
        for event in incoming:
            coord = coordinate(event)
            if coord in ledger:
                if event["raw_hash"] != ledger[coord]["raw_hash"]:
                    raise ContractError("Transport coordinate reused with different bytes")
                continue
            key = checkpoint_key(event)
            expected = offsets.get(key, 0)
            if event["offset"] < expected:
                raise ContractError("Unseen offset behind checkpoint")
            # Gaps are allowed: Kafka compacted topics need not have dense offsets.
            offsets[key] = event["offset"] + 1
            ledger[coord] = event
            added += 1
        if not added:
            with self.store.control() as db:
                db.execute(
                    "UPDATE attempts SET status='replayed',generation=? WHERE id=?",
                    (generation, attempt),
                )
            return {"generation": generation, "added": 0, "replayed": len(incoming)}
        events = sorted(ledger.values(), key=coordinate)
        model = materialize(events)
        evolved = bool(previous and previous["evolved"]) or any(e["evolve"] for e in events)
        snapshots = {}
        for name in TABLES:
            snapshots[name] = self.store.write(name, model[name], evolved)
            if name == fail_after:
                raise RuntimeError(f"Injected failure after {name} snapshot")
        manifest = {
            "generation": generation + 1,
            "input_sha": sha,
            "snapshots": snapshots,
            "next_offsets": offsets,
            "evolved": evolved,
            "counts": {name: len(rows) for name, rows in model.items()},
            "model_version": MODEL_VERSION,
        }
        # Validate the persisted candidates, not just the in-memory transformation.
        for name in TABLES:
            actual = self.store.read(name, manifest).to_pylist()
            expected = model[name]
            if name in ("current", "history") and not evolved:
                expected = [{k: v for k, v in row.items() if k != "region"} for row in expected]
            if sorted(map(canonical, actual)) != sorted(map(canonical, expected)):
                raise RuntimeError("Candidate read-back mismatch")
        if fail_after == "validation":
            raise RuntimeError("Injected failure before publication")
        self.store.publish(manifest, generation, attempt)
        return {
            "generation": generation + 1,
            "added": added,
            "replayed": len(incoming) - added,
            "counts": manifest["counts"],
        }
