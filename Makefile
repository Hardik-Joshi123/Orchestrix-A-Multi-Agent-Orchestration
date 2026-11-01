.PHONY: install dev up down run api ui diagnostics eval worker test lint fmt clean
.PHONY: docker-build docker-up docker-down docker-logs

install:
	pip install -e .

dev:
	pip install -e ".[dev]"

up:
	docker compose up -d redis api worker

down:
	docker compose down

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

run:
	python -m orchestrator "$(TASK)"

api:
	python -m orchestrator --serve

ui:
	streamlit run ui/review_app.py

diagnostics:
	python -m orchestrator --diagnostics

eval:
	python -m orchestrator --eval

worker:
	celery -A orchestrator.worker.celery_app worker --loglevel=info

test:
	pytest

lint:
	ruff check src tests

fmt:
	ruff format src tests

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache **/__pycache__ chroma_data orchestrator_checkpoints.db
