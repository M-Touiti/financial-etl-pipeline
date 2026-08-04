.PHONY: install dev test lint docker-up

install:
	pip install -r requirements.txt

dev:
	uvicorn app.main:app --reload --port 8000

test:
	pytest tests/ -v --cov=app --cov-report=term-missing

test-unit:
	pytest tests/unit/ -v

lint:
	ruff check app/ tests/
	ruff format app/ tests/ --check

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

produce-events:
	python scripts/produce_sample_events.py --count 100

migrate:
	alembic upgrade head

migration:
	alembic revision --autogenerate -m "$(name)"
