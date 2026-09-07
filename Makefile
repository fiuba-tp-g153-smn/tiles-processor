# Makefile for managing the Data Service application

# Declare phony targets to avoid conflicts with files of the same name
.PHONY: up down test test-host clean prod metrics-api

# `make test` runs the suite inside the runtime image. Two of its dependencies
# are native and pip cannot supply them: the GDAL CLI tools (gdalwarp,
# gdal_translate, gdal2tiles.py) from the base image, and libeccodes0 — which
# cfgrib loads by ctypes to decode ECMWF/GFS GRIB — from the Dockerfile. On a
# host missing either, 95 tests fail with "unrecognized engine 'cfgrib'" or
# "No such file or directory: 'gdalwarp'", neither of which names the cause.
#
# The image carries production deps only, so the test tools are installed per
# run into the container's throwaway layer; the host pip cache is mounted so a
# warm run costs a few seconds. LOG_LEVEL/DATA_DIR are passed explicitly because
# the Dockerfile's `ARG LOG_LEVEL` + `ENV LOG_LEVEL=$LOG_LEVEL` leaves them set
# but EMPTY in the image, and conftest's os.environ.setdefault cannot override a
# key that already exists.
TEST_IMAGE  ?= tiles-processor:latest
TEST_PKGS   ?= "pytest>=9.0.2,<10" "pytest-asyncio>=1.3,<2" "pytest-cov>=7,<8" httpx2
PYTEST_ARGS ?= tests/ -m "not skip"

up:
	docker compose -f docker-compose-dev.yaml up --build

down:
	docker compose down
	docker compose -f docker-compose-dev.yaml down --remove-orphans

prod:
	docker compose up --build

test:
	@mkdir -p reports .cache/pip
	@docker image inspect $(TEST_IMAGE) >/dev/null 2>&1 || docker build -t $(TEST_IMAGE) .
	docker run --rm \
	  -v "$(CURDIR)":/w -w /w \
	  -v "$(CURDIR)/.cache/pip":/root/.cache/pip \
	  -e PYTHONPATH=/w/src -e LOG_LEVEL=INFO -e DATA_DIR=/tmp/tiles-processor-test \
	  $(TEST_IMAGE) sh -c 'set -e; \
	    pip install -q $(TEST_PKGS); \
	    mkdir -p "$$DATA_DIR"; \
	    exec python -m pytest $(PYTEST_ARGS) --color=yes \
	      -o cache_dir=/tmp/pytest_cache \
	      --junitxml=reports/junit_report.xml \
	      --cov=src --cov-report term --cov-report html:reports/coverage \
	      -W ignore::DeprecationWarning'

# Same suite on the host: faster to iterate, but silently skips nothing — the
# GRIB and GDAL-dependent tests fail outright unless libeccodes0 and the GDAL
# CLI tools are installed locally. Use `make test` before trusting a green run.
test-host:
	pytest tests/ -m "not skip" --color=yes --junitxml=reports/junit_report.xml --cov=src --cov-report term --cov-report html:reports/coverage -W ignore::DeprecationWarning

metrics-api:
	docker compose -f docker-compose-dev.yaml up --build metrics-api

clean:
	docker volume rm tiles-processor_s3_data || true
	docker volume rm tiles-processor_tiles_data || true
	docker volume rm tiles-processor_rabbitmq_data || true
	docker volume rm tiles-processor_rabbitmq_dev_data || true
	docker volume rm tiles-processor_seaweedfs_filerldb2 || true

precommit:
	pre-commit run --all-files
