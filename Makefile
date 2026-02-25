.PHONY: help setup up down start-producer start-consumer start-pipeline \
        start stop dashboard grafana prometheus kafka-ui alertmanager \
        dlq-retry dq-report test lint clean logs

# ─── Variables ──────────────────────────────────────────────────────────────
PYTHON := python
PIP := pip
DOCKER_COMPOSE := docker-compose -f docker-compose_kafka.yml
PYTEST := pytest

# Service URLs
GRAFANA_URL      := http://localhost:3000
PROMETHEUS_URL   := http://localhost:9090
ALERTMANAGER_URL := http://localhost:9093
KAFKA_UI_URL     := http://localhost:8080
METRICS_URL      := http://localhost:8000/metrics

# ─── Help ───────────────────────────────────────────────────────────────────
help: ## Show this help message
	@echo "Real-Time Customer Heartbeat Monitoring System"
	@echo "=============================================="
	@echo ""
	@powershell -NoProfile -Command "Get-Content $(MAKEFILE_LIST) | Select-String '^[a-zA-Z_-]+:.*?## .*$$' | ForEach-Object { $$_ -match '^([a-zA-Z_-]+):.*?## (.*)$$' | Out-Null; '  {0,-20} {1}' -f $$Matches[1], $$Matches[2] } | Sort-Object"

# ─── Setup ──────────────────────────────────────────────────────────────────
setup: ## Install Python dependencies
	$(PIP) install -r requirements.txt
	@echo "Setup complete!"

setup-dev: ## Install development dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -e ".[dev,dashboard]"
	@echo "Development setup complete!"

# ─── Docker ─────────────────────────────────────────────────────────────────
up: ## Start all infrastructure (Kafka, PostgreSQL, Prometheus, Grafana, Kafka UI)
	$(DOCKER_COMPOSE) up -d
	@echo "Waiting for services to be healthy..."
	@sleep 15
	@echo "Infrastructure is ready!"
	@echo "  Grafana       → $(GRAFANA_URL)  (admin/admin)"
	@echo "  Prometheus    → $(PROMETHEUS_URL)"
	@echo "  Alertmanager  → $(ALERTMANAGER_URL)"
	@echo "  Kafka UI      → $(KAFKA_UI_URL)"

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

infra-logs-kafka: ## View Kafka broker logs
	$(DOCKER_COMPOSE) logs -f --tail=100 kafka

infra-logs-grafana: ## View Grafana logs
	$(DOCKER_COMPOSE) logs -f --tail=100 grafana

infra-logs-prometheus: ## View Prometheus logs
	$(DOCKER_COMPOSE) logs -f --tail=100 prometheus

# ─── Pipeline ───────────────────────────────────────────────────────────────
start-pipeline: ## Start the full pipeline (producer + consumer)
	$(PYTHON) -m src.main --mode full

start: ## Start the pipeline in the background
	@echo "Starting pipeline in the background..."
	@powershell -NoProfile -Command "Start-Process -FilePath '$(PYTHON)' -ArgumentList '-m','src.main','--mode','full' -WindowStyle Hidden"
	@echo "Pipeline started! Use 'make stop' to stop it."

stop: ## Stop the running pipeline
	@echo "Stopping pipeline..."
	-@powershell -NoProfile -Command "Get-WmiObject Win32_Process -ErrorAction SilentlyContinue | Where-Object { $$_.CommandLine -and $$_.CommandLine -match 'src.main' } | ForEach-Object { Stop-Process -Id $$_.ProcessId -Force -ErrorAction SilentlyContinue; Write-Host ('  Killed PID ' + $$_.ProcessId) }"
	@echo "Done."

start-producer: ## Start only the data producer
	$(PYTHON) -m src.main --mode producer

start-consumer: ## Start only the data consumer
	$(PYTHON) -m src.main --mode consumer

dlq-retry: ## Reprocess messages from the Dead Letter Queue
	$(PYTHON) -m src.main --mode dlq-retry

dq-report: ## Print on-demand data quality status report
	$(PYTHON) -m src.main --mode dq-report

# ─── Dashboards & UIs ──────────────────────────────────────────────────────
dashboard: ## Open Grafana dashboard in the browser (default admin/admin)
	@echo "Opening Grafana dashboard at $(GRAFANA_URL) ..."
	@start "" "$(GRAFANA_URL)"

grafana: ## Open Grafana in the browser (default admin/admin)
	@echo "Opening Grafana at $(GRAFANA_URL) ..."
	@start "" "$(GRAFANA_URL)"

prometheus: ## Open Prometheus in the browser
	@echo "Opening Prometheus at $(PROMETHEUS_URL) ..."
	@start "" "$(PROMETHEUS_URL)"

alertmanager: ## Open Alertmanager in the browser
	@echo "Opening Alertmanager at $(ALERTMANAGER_URL) ..."
	@start "" "$(ALERTMANAGER_URL)"

kafka-ui: ## Open Kafka UI in the browser
	@echo "Opening Kafka UI at $(KAFKA_UI_URL) ..."
	@start "" "$(KAFKA_UI_URL)"

open-all-uis: ## Open all UIs (Grafana + Prometheus + Kafka UI + Alertmanager)
	@echo "Opening all monitoring UIs..."
	@start "" "$(GRAFANA_URL)"
	@start "" "$(PROMETHEUS_URL)"
	@start "" "$(KAFKA_UI_URL)"
	@start "" "$(ALERTMANAGER_URL)"
	@echo "  Grafana       → $(GRAFANA_URL)"
	@echo "  Prometheus    → $(PROMETHEUS_URL)"
	@echo "  Kafka UI      → $(KAFKA_UI_URL)"
	@echo "  Alertmanager  → $(ALERTMANAGER_URL)"

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
	@powershell -NoProfile -Command "Get-ChildItem -Path . -Filter __pycache__ -Recurse -Directory -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force"
	@powershell -NoProfile -Command "Get-ChildItem -Path . -Filter .pytest_cache -Recurse -Directory -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force"
	@powershell -NoProfile -Command "Remove-Item -Path htmlcov,.coverage,coverage.xml -Recurse -Force -ErrorAction SilentlyContinue"
	@powershell -NoProfile -Command "Remove-Item -Path logs/*.log -Force -ErrorAction SilentlyContinue"
	@echo "Cleaned up!"

logs: ## View pipeline logs (live tail)
	@powershell -NoProfile -Command "if (Test-Path logs/pipeline.log) { Get-Content logs/pipeline.log -Tail 50 -Wait } else { Write-Host 'No log file found. Start the pipeline first.' }"
