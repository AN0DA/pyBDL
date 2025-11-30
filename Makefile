.PHONY: lint mypy test docs

all: lint mypy test

lint:
	uv run ruff format pyldb/ tests/ \
	&& uv run ruff check --fix --show-fixes pyldb/ tests/ \
	&& uv run bandit -c pyproject.toml -r pyldb/

mypy:
	uv run mypy pyldb/ tests/

test:
	uv run pytest --cov --cov-report term-missing:skip-covered

docs:
	uv run sphinx-build -W --keep-going -b html docs docs/_build/html
