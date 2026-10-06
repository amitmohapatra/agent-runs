.DEFAULT_GOAL := help
.PHONY: help install lint typecheck test sdk coverage examples links openapi migrate image up down

help: ## Show this help
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## Sync the dev environment (agent-contracts from ../agent-contracts, the SDK from sdk/python)
	uv sync --all-extras

lint: ## Ruff check and format check
	uv run ruff check src tests alembic sdk/python examples scripts
	uv run ruff format --check src tests alembic sdk/python examples scripts

typecheck: ## Pyright
	uv run pyright

test: ## Run the service's suite (needs the local PostgreSQL), then the SDK's
	uv run pytest -q
	uv run pytest -q sdk/python/tests

sdk: ## The SDK's suite with line and branch coverage, failing under 100%
	uv run pytest -q sdk/python/tests --cov=trellis.runs --cov-branch --cov-report=term-missing --cov-fail-under=100

coverage: ## The suite with line and branch coverage, failing under 95%
	uv run pytest -q --cov --cov-report=term-missing --cov-fail-under=95

examples: ## Run every numbered example in process (the local PostgreSQL, as for test)
	@for f in examples/[0-9]*.py; do \
		echo "== $$f"; uv run python "$$f" || exit 1; \
	done

links: ## Fail on a broken relative link or anchor in any Markdown file
	python3 scripts/check_links.py .

openapi: ## Rewrite docs/openapi.json from the code (commit it with the change)
	uv run python -m agent_runs.tools.export_openapi docs/openapi.json

migrate: ## Bring the database to the latest schema
	uv run alembic upgrade head

image: ## Build the container image
	docker build --build-context contracts=../agent-contracts -t agent-runs:dev .

up: ## Start the database, the migration, the API and the ticker
	docker compose up -d --build

down: ## Stop everything and drop the volumes
	docker compose down -v
