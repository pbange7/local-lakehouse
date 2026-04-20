# =============================================================================
# local-lakehouse — Makefile
# Requires: docker compose v2 (not docker-compose v1)
# Platform: darwin/arm64 (Apple Silicon)
# One-time Python setup: pip install -e ".[test]"
# =============================================================================

SHELL          := /bin/bash
.DEFAULT_GOAL  := help

COMPOSE        := docker compose
PROJECT_DIR    := $(shell pwd)
ENV_FILE       := $(PROJECT_DIR)/.env
SCRIPTS_DIR    := $(PROJECT_DIR)/scripts

GREEN  := \033[0;32m
RED    := \033[0;31m
YELLOW := \033[1;33m
RESET  := \033[0m

# =============================================================================
# INTERNAL — not shown in help
# =============================================================================

.PHONY: _check-env
_check-env:
	@if [ ! -f "$(ENV_FILE)" ]; then \
	  printf "$(RED)[ERROR]$(RESET) .env not found. Run: make seed\n"; \
	  exit 1; \
	fi
	@if grep -q "KAFKA_CLUSTER_ID=REPLACE_ME" "$(ENV_FILE)"; then \
	  printf "$(RED)[ERROR]$(RESET) KAFKA_CLUSTER_ID=REPLACE_ME in .env. Run: make seed\n"; \
	  exit 1; \
	fi

# =============================================================================
# SETUP
# =============================================================================

.PHONY: seed
seed: ## Generate KAFKA_CLUSTER_ID into .env (safe to re-run)
	@if [ ! -f "$(ENV_FILE)" ]; then \
	  cp "$(ENV_FILE).example" "$(ENV_FILE)"; \
	  printf "$(GREEN)[INFO]$(RESET) Created .env from .env.example\n"; \
	fi
	@if grep -q "KAFKA_CLUSTER_ID=REPLACE_ME" "$(ENV_FILE)"; then \
	  NEW_ID=$$($(COMPOSE) run --rm --no-deps kafka /opt/kafka/bin/kafka-storage.sh random-uuid 2>/dev/null | tr -d '\r\n'); \
	  sed -i.bak "s|KAFKA_CLUSTER_ID=REPLACE_ME|KAFKA_CLUSTER_ID=$$NEW_ID|" "$(ENV_FILE)" && rm -f "$(ENV_FILE).bak"; \
	  printf "$(GREEN)[INFO]$(RESET) KAFKA_CLUSTER_ID set to $$NEW_ID\n"; \
	else \
	  printf "$(YELLOW)[INFO]$(RESET) KAFKA_CLUSTER_ID already set — nothing to do.\n"; \
	fi

# =============================================================================
# LIFECYCLE
# =============================================================================

.PHONY: up
up: _check-env ## Start core services without streaming profile (add --profile flink or bento separately)
	$(COMPOSE) up -d
	@printf "Waiting for services to be healthy...\n"
	@for svc in kafka minio nessie clickhouse trino api ui producer; do \
	  timeout=60; \
	  while [ $$timeout -gt 0 ]; do \
	    status=$$($(COMPOSE) ps --format json $$svc 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Health','running'))" 2>/dev/null || echo "running"); \
	    if [ "$$status" = "healthy" ] || [ "$$status" = "running" ]; then \
	      printf "  $(GREEN)✓$(RESET) $$svc\n"; break; \
	    fi; \
	    sleep 2; timeout=$$((timeout-2)); \
	  done; \
	done

.PHONY: flink-up
flink-up: _check-env ## Start core services + Flink streaming profile
	$(COMPOSE) --profile flink up -d --build

.PHONY: bento-up
bento-up: _check-env ## Start core services + Bento streaming profile
	$(COMPOSE) --profile bento up -d --build

.PHONY: down
down: ## Stop and remove containers (volumes preserved)
	$(COMPOSE) --profile flink --profile bento down

.PHONY: restart
restart: _check-env ## Restart all services
	$(COMPOSE) restart

.PHONY: status
status: ## Show container status and health
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs for all services (Ctrl-C to exit)
	$(COMPOSE) logs -f

.PHONY: logs-kafka
logs-kafka: ## Tail Kafka logs
	$(COMPOSE) logs -f kafka

.PHONY: logs-producer
logs-producer: ## Tail producer logs
	$(COMPOSE) logs -f producer

.PHONY: logs-minio
logs-minio: ## Tail MinIO logs
	$(COMPOSE) logs -f minio

.PHONY: logs-nessie
logs-nessie: ## Tail Nessie catalog logs
	$(COMPOSE) logs -f nessie

.PHONY: logs-flink-jm
logs-flink-jm: ## Tail Flink JobManager logs
	$(COMPOSE) --profile flink logs -f flink-jobmanager

.PHONY: logs-flink-tm
logs-flink-tm: ## Tail Flink TaskManager logs
	$(COMPOSE) --profile flink logs -f flink-taskmanager

.PHONY: logs-bento
logs-bento: ## Tail Bento logs
	$(COMPOSE) --profile bento logs -f bento

.PHONY: logs-pyiceberg
logs-pyiceberg: ## Tail PyIceberg writer logs
	$(COMPOSE) --profile bento logs -f pyiceberg-writer

.PHONY: logs-clickhouse
logs-clickhouse: ## Tail ClickHouse logs
	$(COMPOSE) logs -f clickhouse

.PHONY: logs-trino
logs-trino: ## Tail Trino logs
	$(COMPOSE) logs -f trino

.PHONY: logs-api
logs-api: ## Tail FastAPI backend logs
	$(COMPOSE) logs -f api

.PHONY: logs-ui
logs-ui: ## Tail Web UI (nginx) logs
	$(COMPOSE) logs -f ui

# =============================================================================
# BUILD
# =============================================================================

.PHONY: build
build: _check-env ## Build all services that have a build context
	$(COMPOSE) build

.PHONY: build-producer
build-producer: _check-env ## Build only the producer image
	$(COMPOSE) build producer

.PHONY: build-flink
build-flink: _check-env ## Build custom Flink image (downloads Kafka + Iceberg JARs)
	$(COMPOSE) build flink-jobmanager

.PHONY: build-api
build-api: ## Build the FastAPI backend image
	$(COMPOSE) build api

.PHONY: build-ui
build-ui: ## Build the Web UI image (nginx serving static files)
	$(COMPOSE) build ui

.PHONY: build-no-cache
build-no-cache: _check-env ## Rebuild all images without Docker layer cache
	$(COMPOSE) build --no-cache

# =============================================================================
# TESTING
# =============================================================================

.PHONY: smoke
smoke: ## Run Kafka broker smoke test (requires running stack)
	@bash "$(SCRIPTS_DIR)/smoke-test-kafka.sh"

.PHONY: smoke-minio
smoke-minio: ## Run MinIO S3 smoke test (requires running stack)
	@bash "$(SCRIPTS_DIR)/smoke-test-minio.sh"

.PHONY: smoke-nessie
smoke-nessie: ## Run Nessie Iceberg catalog smoke test (requires running stack)
	@bash "$(SCRIPTS_DIR)/smoke-test-nessie.sh"

.PHONY: flink-submit
flink-submit: ## Submit the Flink SQL streaming job (Kafka → Iceberg)
	@bash "$(SCRIPTS_DIR)/submit-flink-job.sh"

.PHONY: smoke-flink
smoke-flink: ## Run Flink end-to-end smoke test (job + checkpoint + MinIO + Nessie)
	@bash "$(SCRIPTS_DIR)/smoke-test-flink.sh"

.PHONY: clickhouse-submit
clickhouse-submit: ## Submit the Flink SQL job (Kafka → ClickHouse)
	@bash "$(SCRIPTS_DIR)/submit-clickhouse-job.sh"

.PHONY: smoke-clickhouse
smoke-clickhouse: ## Run ClickHouse smoke test (health + row count + aggregate)
	@bash "$(SCRIPTS_DIR)/smoke-test-clickhouse.sh"

.PHONY: smoke-trino
smoke-trino: ## Run Trino smoke test (Iceberg + ClickHouse + cross-catalog join)
	@bash "$(SCRIPTS_DIR)/smoke-test-trino.sh"

.PHONY: smoke-duckdb
smoke-duckdb: ## Run DuckDB Iceberg query via one-shot Python container
	@bash "$(SCRIPTS_DIR)/smoke-test-duckdb.sh"

.PHONY: smoke-api
smoke-api: ## Run FastAPI backend smoke test
	@bash "$(SCRIPTS_DIR)/smoke-test-api.sh"

.PHONY: smoke-ui
smoke-ui: ## Run Web UI smoke test (nginx + API proxy)
	@bash "$(SCRIPTS_DIR)/smoke-test-ui.sh"

.PHONY: smoke-bento
smoke-bento: ## Run Bento pipeline smoke test (health + CH rows + Iceberg files)
	@bash "$(SCRIPTS_DIR)/smoke-test-bento.sh"

.PHONY: smoke-all
smoke-all: ## Run consolidated end-to-end integration test (all 11 components)
	@bash "$(SCRIPTS_DIR)/smoke-test-all.sh"

.PHONY: verify
verify: ## Verify producer is publishing messages (requires running stack)
	@bash "$(SCRIPTS_DIR)/verify-producer.sh"

.PHONY: test-unit
test-unit: ## Run unit tests — no Docker required
	PYTHONPATH="$(PROJECT_DIR)/producer" pytest -m unit -v

.PHONY: test-integration
test-integration: ## Run integration tests — requires Kafka at localhost:9094
	PYTHONPATH="$(PROJECT_DIR)/producer" pytest -m integration -v

.PHONY: test
test: test-unit smoke-all test-integration ## Run the full test suite (unit + consolidated smoke + integration)

# =============================================================================
# CLEANUP
# =============================================================================

.PHONY: clean
clean: ## Stop containers and delete volumes — DATA LOSS
	$(COMPOSE) down -v

.PHONY: clean-build
clean-build: ## Remove Docker build cache for project images
	$(COMPOSE) build --no-cache 2>/dev/null; \
	docker image prune -f --filter "label=com.docker.compose.project=lakehouse"

# =============================================================================
# HELP
# =============================================================================

.PHONY: help
help: ## Show this help
	@echo ""
	@echo "  local-lakehouse — available targets"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | grep -v '^_' \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""
