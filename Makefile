COMPOSE := docker compose -f infra/docker-compose.yml
CONNECT := http://localhost:8083
PYTHON  ?= python3

.PHONY: help install lint test check up down logs connectors status setup deploy sync monitor

help:            ## Show this help
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install:         ## Install the Python package with dev tools (into the active virtualenv)
	$(PYTHON) -m pip install -e '.[dev]'

lint:            ## Ruff lint
	ruff check src tests pipelines powerbi

test:            ## Unit tests
	pytest -q

check: lint test ## Lint + tests

setup:           ## One-off Databricks setup: schema, bronze table, service principal, Zerobus endpoint
	darwin-setup

deploy:          ## Deploy the silver/gold pipeline and its hourly job, then run it once
	darwin-deploy-pipeline --run-now

sync:            ## Load today's Darwin reference + timetable data once
	darwin-reference-sync --once

monitor:         ## Watch the live Darwin feed for 60s (STOMP)
	darwin-feed-monitor --seconds 60

up:              ## Start Kafka, Connect, Kafka UI, the Streams app and the reference sync
	$(COMPOSE) up -d --build

down:            ## Stop the local stack (Kafka data is kept in a volume)
	$(COMPOSE) down

logs:            ## Follow logs of the custom services
	$(COMPOSE) logs -f darwin-zerobus darwin-reference-sync

connectors:      ## Register the Darwin ActiveMQ source connector (waits for Connect)
	@until curl -sf $(CONNECT)/connector-plugins >/dev/null; do echo "waiting for Connect..."; sleep 5; done
	@curl -s -X PUT -H 'Content-Type: application/json' $(CONNECT)/connectors/darwin-pushport/config \
		-d "$$(python3 -c 'import json; print(json.dumps(json.load(open("infra/connect/darwin-pushport-connector.json"))["config"]))')" \
		>/dev/null && echo "darwin-pushport connector registered"

status:          ## Connector + consumer lag status
	@curl -s $(CONNECT)/connectors/darwin-pushport/status | python3 -m json.tool
	@$(COMPOSE) exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group darwin-zerobus 2>/dev/null | tail -n +2
