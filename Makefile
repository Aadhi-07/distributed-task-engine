.PHONY: help build up down restart logs test migrate makemigrations lint clean

help:
	@echo "Available commands:"
	@echo "  make build          Build docker containers"
	@echo "  make up             Start all containers in detached mode"
	@echo "  make down           Stop and remove containers"
	@echo "  make restart        Restart containers"
	@echo "  make logs           View live container logs"
	@echo "  make migrate        Apply database migrations"
	@echo "  make test           Run test suite"
	@echo "  make clean          Clean temp files and pycache"

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

migrate:
	alembic upgrade head

test:
	pytest -v

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
