# Makefile for managing the Data Service application

# Declare phony targets to avoid conflicts with files of the same name
.PHONY: up install

up:
	docker compose -f docker-compose-dev.yml up --build

down:
	docker compose down
	docker compose -f docker-compose-dev.yml down --remove-orphans

prod:
	docker compose up --build
