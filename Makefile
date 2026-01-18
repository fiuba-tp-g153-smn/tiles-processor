# Makefile for managing the Data Service application

# Declare phony targets to avoid conflicts with files of the same name
.PHONY: up down test clean prod

up:
	docker compose -f docker-compose-dev.yaml up --build

down:
	docker compose down
	docker compose -f docker-compose-dev.yaml down --remove-orphans

prod:
	docker compose up --build

test:
	pytest tests/ -m "not skip" --color=yes --junitxml=reports/junit_report.xml --cov=src --cov-report term --cov-report html:reports/coverage -W ignore::DeprecationWarning

clean:
	docker volume rm tiles-processor_minio_data || true
	docker volume rm tiles-processor_minio_dev_data || true
	docker volume rm tiles-processor_tiles_data || true
