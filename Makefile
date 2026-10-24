.PHONY: install lint test demo kafka-smoke build hygiene
install:
	uv sync --frozen --extra dev
lint:
	uv run --frozen ruff check src tests scripts
	uv run --frozen ruff format --check src tests scripts
test:
	uv run --frozen pytest --cov --cov-fail-under=85 --cov-report=term-missing
demo:
	uv run --frozen retail --root data/demo demo --count 1000
kafka-smoke:
	uv run --frozen python scripts/kafka_smoke.py
build:
	uv build
hygiene:
	uv run --frozen python scripts/check_repo.py
