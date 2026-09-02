.PHONY: install test lint format up down produce consume

install:
	pip install -e ".[dev]"

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/
	mypy src/

format:
	ruff format src/ tests/

up:
	docker compose up -d

down:
	docker compose down

produce:
	python -m event_pipeline.producer.run

consume:
	python -m event_pipeline.consumer.run
