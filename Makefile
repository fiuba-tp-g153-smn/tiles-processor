# Makefile for managing the Data Service application

# Declare phony targets to avoid conflicts with files of the same name
.PHONY: up down test clean prod radar-build radar-run

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
	docker volume rm tiles-processor_s3_data || true
	docker volume rm tiles-processor_tiles_data || true
	docker volume rm tiles-processor_rabbitmq_data || true
	docker volume rm tiles-processor_rabbitmq_dev_data || true
	docker volume rm tiles-processor_seaweedfs_filerldb2 || true

precommit:
	pre-commit run --all-files

radar-build:
	docker build -f Dockerfile.script -t radar-tiles-processor .

radar-run:
	mkdir -p output_radar
	docker run --rm \
		-v $(PWD):/data:ro \
		-v $(PWD)/output_radar:/app/output_radar \
		radar-tiles-processor
