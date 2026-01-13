# Makefile for managing the Data Service application

# Declare phony targets to avoid conflicts with files of the same name
.PHONY: up install

install:
	pip install poetry
	poetry install

up:
	docker compose up --build
