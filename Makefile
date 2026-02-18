.PHONY: help setup up down start-producer start-consumer start-pipeline dashboard test lint clean logs

# ─── Variables ──────────────────────────────────────────────────────────────
PYTHON := python
PIP := pip
DOCKER_COMPOSE := docker-compose
PYTEST := pytest
STREAMLIT := streamlit

# ─── Help ───────────────────────────────────────────────────────────────────
help: ## Show this help message
	@echo "Real-Time Customer Heartbeat Monitoring System"
	@echo "=============================================="
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Setup ──────────────────────────────────────────────────────────────────
setup: ## Install Python dependencies
	$(PIP) install -r requirements.txt
	@echo "Setup complete!"

setup-dev: ## Install development dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -e ".[dev,dashboard]"
	@echo "Development setup complete!"

# ─── Docker ─────────────────────────────────────────────────────────────────
up: ## Start all infrastructure (Kafka, Zookeeper, PostgreSQL, Grafana)
	$(DOCKER_COMPOSE) up -d
	@echo "Waiting for services to be healthy..."
	@sleep 15
	@echo "Infrastructure is ready!"

down: ## Stop all infrastructure
	$(DOCKER_COMPOSE) down
	@echo "Infrastructure stopped."

down-clean: ## Stop infrastructure and remove volumes
	$(DOCKER_COMPOSE) down -v
	@echo "Infrastructure stopped and volumes removed."

infra-status: ## Check status of infrastructure services
	$(DOCKER_COMPOSE) ps

infra-logs: ## View infrastructure logs
	$(DOCKER_COMPOSE) logs -f --tail=50

# ─── Pipeline ───────────────────────────────────────────────────────────────
start-pipeline: ## Start the full pipeline (producer + consumer)
	$(PYTHON) -m src.main --mode full

start-producer: ## Start only the data producer
	$(PYTHON) -m src.main --mode producer

start-consumer: ## Start only the data consumer
	$(PYTHON) -m src.main --mode consumer

# ─── Dashboard ──────────────────────────────────────────────────────────────
dashboard: ## Launch the Streamlit dashboard
	$(STREAMLIT) run src/dashboard/app.py --server.port 8501

# ─── Testing ────────────────────────────────────────────────────────────────
test: ## Run all unit tests
	$(PYTEST) tests/ -v --ignore=tests/test_integration.py

test-integration: ## Run integration tests (requires infrastructure)
	$(PYTEST) tests/test_integration.py -v -m integration

test-all: ## Run all tests
	$(PYTEST) tests/ -v

test-coverage: ## Run tests with coverage report
	$(PYTEST) tests/ -v --cov=src --cov-report=html --cov-report=term-missing

test-script: ## Run the standalone test script
	$(PYTHON) tests/test_pipeline_script.py

# ─── Code Quality ───────────────────────────────────────────────────────────
lint: ## Run linter
	flake8 src/ tests/ --max-line-length=100

format: ## Format code with black and isort
	black src/ tests/ --line-length=100
	isort src/ tests/ --profile=black --line-length=100

typecheck: ## Run type checking
	mypy src/ --ignore-missing-imports

# ─── Utilities ──────────────────────────────────────────────────────────────
clean: ## Clean up generated files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf htmlcov .coverage coverage.xml
	rm -rf logs/*.log
	@echo "Cleaned up!"

logs: ## View pipeline logs
	@tail -f logs/pipeline.log 2>/dev/null || echo "No log file found. Start the pipeline first."
