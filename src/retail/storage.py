"""Iceberg single-table commits and separate, atomic multi-table publication."""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
from filelock import FileLock
from pyiceberg.catalog import load_catalog
from pyiceberg.types import StringType

TABLES = ("changes", "current", "history", "gold")
BASE = [
    ("order_id", pa.string()),
    ("customer_id", pa.string()),
    ("revision", pa.int64()),
    ("amount_minor", pa.int64()),
    ("currency", pa.string()),
    ("status", pa.string()),
    ("updated_at", pa.string()),
    ("deleted", pa.bool_()),
]


def schema(name, evolved=False):
    if name == "changes":
        fields = [
            ("topic", pa.string()),
            ("partition", pa.int64()),
            ("offset", pa.int64()),
            ("raw_hash", pa.string()),
            ("raw", pa.string()),
        ]
    elif name == "gold":
        fields = [
            ("date", pa.string()),
            ("currency", pa.string()),
            ("status", pa.string()),
            ("orders", pa.int64()),
            ("amount_minor", pa.int64()),
        ]
    else:
        fields = BASE + ([("valid_to_revision", pa.int64())] if name == "history" else [])
        if evolved:
            fields = fields + [("region", pa.string())]
    return pa.schema(fields)


class Store:
    def __init__(self, root, initialize=True):
        self.root = Path(root).resolve()
        if initialize:
            self.root.mkdir(parents=True, exist_ok=True)
        self.lock = FileLock(str(self.root / "writer.lock"), timeout=0)
        self.control_path = self.root / "control.sqlite"
        self.catalog = load_catalog(
            "retail",
            type="sql",
            uri=f"sqlite:///{self.root / 'catalog.sqlite'}",
            warehouse=(self.root / "warehouse").as_uri(),
            init_catalog_tables="true" if initialize else "false",
        )
        if not initialize:
            return
        with self.lock, self.control() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                "CREATE TABLE IF NOT EXISTS publications "
                "(generation INTEGER PRIMARY KEY, manifest TEXT NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS attempts "
                "(id INTEGER PRIMARY KEY, input_sha TEXT NOT NULL, "
                "status TEXT NOT NULL, generation INTEGER, "
                "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            )
            self.catalog.create_namespace_if_not_exists("retail")
            for name in TABLES:
                self.catalog.create_table_if_not_exists(f"retail.{name}", schema(name))

    @contextmanager
    def control(self):
        db = sqlite3.connect(self.control_path, timeout=10)
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def publication(self, generation=None):
        with self.control() as db:
            if generation is None:
                row = db.execute(
                    "SELECT manifest FROM publications ORDER BY generation DESC LIMIT 1"
                ).fetchone()
            else:
                row = db.execute(
                    "SELECT manifest FROM publications WHERE generation=?", (generation,)
                ).fetchone()
                if row is None:
                    raise ValueError("Unknown generation")
        return json.loads(row[0]) if row else None

    def read(self, name, publication):
        if name not in TABLES:
            raise ValueError("Unknown table")
        snapshot = publication["snapshots"][name] if publication else None
        if snapshot is None:
            return pa.Table.from_pylist(
                [], schema=schema(name, bool(publication and publication["evolved"]))
            )
        return self.catalog.load_table(f"retail.{name}").scan(snapshot_id=snapshot).to_arrow()

    def write(self, name, rows, evolved):
        table = self.catalog.load_table(f"retail.{name}")
        if (
            name in ("current", "history")
            and evolved
            and "region" not in table.schema().column_names
        ):
            with table.update_schema() as update:
                update.add_column("region", StringType())
        actual_evolved = "region" in table.schema().column_names
        arrow = pa.Table.from_pylist(rows, schema=schema(name, actual_evolved))
        if table.current_snapshot() is None:
            if rows:
                table.append(arrow)
        else:
            table.overwrite(arrow)
        snapshot = table.current_snapshot()
        return snapshot.snapshot_id if snapshot else None

    def publish(self, manifest, expected_generation, attempt):
        with self.control() as db:
            db.execute("BEGIN IMMEDIATE")
            actual = db.execute("SELECT COALESCE(MAX(generation),0) FROM publications").fetchone()[
                0
            ]
            if actual != expected_generation:
                raise RuntimeError("Publication compare-and-swap failed")
            db.execute(
                "INSERT INTO publications VALUES (?,?)",
                (manifest["generation"], json.dumps(manifest, sort_keys=True)),
            )
            db.execute(
                "UPDATE attempts SET status='published',generation=? WHERE id=?",
                (manifest["generation"], attempt),
            )
