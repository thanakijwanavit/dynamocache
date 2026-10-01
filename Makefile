.PHONY: test lint typecheck integration

test:
	pytest --cov=dynamocache

lint:
	ruff check src tests

typecheck:
	mypy

integration:
	DYNAMOCACHE_INTEGRATION=1 pytest -m integration
