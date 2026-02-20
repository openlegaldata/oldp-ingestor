.PHONY: help install test test-real test-cov lint format clean docker-build docker-test

.DEFAULT_GOAL := help

PYTHON ?= python3
VENV := .venv
BIN := $(VENV)/bin
DOCKER_IMAGE ?= oldp-ingestor

# Auto-detect: prefer uv, fall back to pip
UV := $(shell command -v uv 2>/dev/null)

ifdef UV
  SYNC_CMD = uv sync --python $(PYTHON)
else
  VENV_CMD = $(PYTHON) -m venv $(VENV) && $(BIN)/pip install --upgrade pip
  SYNC_CMD = $(VENV_CMD) && $(BIN)/pip install -e ".[dev]"
endif

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies into venv
	$(SYNC_CMD)

test: install ## Run tests
	$(BIN)/pytest

test-real: install ## Run real API tests (network access)
	$(BIN)/pytest --run-real -m real -v

test-cov: install ## Run tests with coverage report
	$(BIN)/pytest --cov --cov-report=term-missing --cov-report=html

lint: install ## Check code style (ruff)
	$(BIN)/ruff check src/ tests/
	$(BIN)/ruff format --check src/ tests/

format: install ## Auto-format code (ruff)
	$(BIN)/ruff check --fix src/ tests/
	$(BIN)/ruff format src/ tests/

clean: ## Remove venv, build artifacts, caches
	rm -rf $(VENV) build/ dist/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +

docker-build: ## Build production Docker image
	docker build -t $(DOCKER_IMAGE) .

docker-test: ## Run tests inside Docker
	docker build -f Dockerfile.test -t $(DOCKER_IMAGE)-test .
	docker run --rm $(DOCKER_IMAGE)-test
