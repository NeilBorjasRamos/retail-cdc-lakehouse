import argparse
import json
import os
from pathlib import Path

from retail.contract import ContractError
from retail.engine import Engine
from retail.generator import write_batches
from retail.kafka import ingest_partition
from retail.query import report


def main(argv=None):
    parser = argparse.ArgumentParser(description="Recoverable retail CDC Iceberg lakehouse")
    parser.add_argument("--root", default="data/lakehouse", help="Local persistent POSIX directory")
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate", help="Write fictional Debezium-shaped inputs")
    generate.add_argument("--output", default="data/input")
    generate.add_argument("--count", type=int, default=1000)
    ingest = commands.add_parser("ingest", help="Apply a JSONL transport batch")
    ingest.add_argument("input")
    query = commands.add_parser("report", help="Reconcile one pinned publication")
    query.add_argument("--generation", type=int)
    demo = commands.add_parser("demo", help="Fresh workspace: full failure and replay drill")
    demo.add_argument("--count", type=int, default=1000)
    kafka = commands.add_parser("kafka", help="Bounded ingestion from one Kafka partition")
    kafka.add_argument("--topic", default="retail.public.orders")
    kafka.add_argument("--partition", type=int, default=0)
    kafka.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            result = {"files": write_batches(args.output, args.count)}
        elif args.command == "ingest":
            result = Engine(args.root).ingest_file(args.input)
        elif args.command == "report":
            result = report(args.root, args.generation)
        elif args.command == "kafka":
            result = ingest_partition(
                args.root,
                os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
                args.topic,
                args.partition,
                args.limit,
            )
        else:
            result = run_demo(args.root, args.count)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 2 if result.get("reconciled") is False else 0
    except Exception as exc:
        # No payloads, credentials, connection strings or stack traces on the CLI.
        message = (
            str(exc) if isinstance(exc, ContractError) else "Operation failed; see the runbook."
        )
        print(json.dumps({"error": type(exc).__name__, "message": message}))
        return 1


def run_demo(root, count):
    root = Path(root)
    if root.exists() and any(root.iterdir()):
        raise ValueError("Demo requires a new empty directory")
    paths = write_batches(root / "input", count)
    engine = Engine(root)
    first = engine.ingest_file(paths[0])
    old = report(root, first["generation"])
    try:
        engine.ingest_file(paths[1], fail_after="current")
    except RuntimeError as exc:
        if "Injected failure" not in str(exc):
            raise
    else:
        raise AssertionError("Failure injection did not trigger")
    assert report(root)["generation"] == first["generation"]
    results = [engine.ingest_file(path) for path in paths[1:]]
    replay = engine.ingest_file(paths[-1])
    assert replay["added"] == 0
    assert report(root, first["generation"]) == old
    result = report(root)
    assert result["reconciled"]
    result["validation"] = {
        "synthetic": True,
        "orders_generated": count,
        "failure_after_current_recovered": True,
        "historical_publication_unchanged": True,
        "replay_added": replay["added"],
        "batches": [first, *results],
    }
    (root / "validation.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
