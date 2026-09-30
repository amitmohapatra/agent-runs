.DEFAULT_GOAL := help
.PHONY: help install lint typecheck test migrate image up down

help: ## Show this help
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## Sync the dev environment (agent-contracts from ../agent-contracts)
	uv sync --all-extras

lint: ## Ruff check and format check
	uv run ruff check src tests alembic
	uv run ruff format --check src tests alembic

typecheck: ## Pyright
	uv run pyright

test: ## Run the test suite (needs the local PostgreSQL)
	uv run pytest -q

migrate: ## Bring the database to the latest schema
	uv run alembic upgrade head

image: ## Build the container image
	docker build --build-context contracts=../agent-contracts -t agent-runs:dev .

up: ## Start the database, the migration, the API and the ticker
	docker compose up -d --build

down: ## Stop everything and drop the volumes
	docker compose down -v
