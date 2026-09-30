.PHONY: install up down infra test lint typecheck check bench bench-smoke

install:            ## install dependencies (needs uv)
	uv sync

infra:              ## start only PostgreSQL and Redis (for tests)
	docker compose up -d --wait postgres redis

up:                 ## run the whole stack: API, scheduler, 3 workers
	docker compose up -d --build --wait

down:
	docker compose down

test: infra
	uv run pytest --cov --cov-fail-under=90

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run mypy

check: lint typecheck test

bench-smoke: up    ## ~1 minute
	uv run python bench/run_benchmark.py --jobs 2000 --rate 12000 --chaos-interval 4 --label smoke

bench: up          ## the resume numbers: 50k jobs at 2,000/min with a worker killed every minute
	uv run python bench/run_benchmark.py --jobs 50000 --rate 2000 --work-ms 250 \
		--p-fail 0.05 --chaos-interval 60 --label sustained-2000pm-chaos
