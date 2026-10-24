# Contributing

Use Python 3.12 and `uv sync --frozen --extra dev`. Keep contract parsing, pure
materialization, storage and transport concerns separate. Add behavioral tests for
changed failure guarantees; avoid tests that merely repeat implementation details.

Before a pull request:

```bash
make lint test
git add .
make hygiene
git diff --cached --stat
uv build
```

For Kafka changes run `scripts/kafka_smoke.py` against a disposable broker and a fresh
SMOKE_ROOT. Never use actual customer data or secrets. Do not commit warehouse files,
input dumps, environment files, logs, coverage, dependencies or built distributions.

Changing materialization meaning requires an explicit MODEL_VERSION decision, a
migration/rebuild procedure, validation of historical reads and an ADR. The version
is a compatibility boundary, not an automatic code hash. Compatible nullable region
addition is already implemented; other schema changes need a design and tests.

Record exactly which checks ran. The CI workflow must pass before claiming remote
validation. Local test success does not imply a tested Docker or cloud deployment.
