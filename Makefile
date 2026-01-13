# Makefile for managing the Data Service application

# Declare phony targets to avoid conflicts with files of the same name
.PHONY: up install test

up:
	docker compose -f docker-compose-dev.yaml up --build

down:
	docker compose down
	docker compose -f docker-compose-dev.yaml down --remove-orphans

prod:
	docker compose up --build

test:
	BAND_13_SCHEDULE_CRON="*/10 * * * *" \
	BAND_9_SCHEDULE_CRON="*/10 * * * *" \
	TZ="UTC" \
	TMP_DIR=".tmp" \
	.venv/bin/pytest tests
