# CEDAR risk module -- test branch: run the module and every test scenario.
# `make` (or `make help`) lists the commands. Settings: the .env file.
#
# Extra arguments go through ARGS, e.g.
#   make request ARGS="--pilots sl --tender EN_N_101_2025"
#   make python ARGS="--pilots it"

COMPOSE   ?= docker compose
PYTHON    ?= python
CONTAINER := cedar-risk
KAFKA     := $(COMPOSE) -f kafka/docker-compose.yml
SYNTHETIC := $(COMPOSE) -f synthetic_anomalies/docker-compose.yml
ARGS      ?=

# the containers run as you, so the files they write in outputs/ and logs/ are yours
export CEDAR_UID := $(shell id -u)
export CEDAR_GID := $(shell id -g)

.DEFAULT_GOAL := help
.PHONY: help run stop restart logs status source-run python \
        kafka-credentials kafka-up kafka-down kafka-logs request results \
        synthetic-up synthetic-down synthetic-test

help: ## list the commands
	@awk 'BEGIN {FS = ":.*## "} /^## / {printf "\n%s\n", substr($$0, 4)} /^[a-z-]+:.*## / {printf "  make %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## The module (the installed package; the broker is KAFKA_BOOTSTRAP_SERVERS in .env)
run: ## build and start the module (replaces a running package or source copy)
	@echo "broker: $$(grep -E '^KAFKA_BOOTSTRAP_SERVERS=' .env | cut -d= -f2-)"
	@docker rm -f $(CONTAINER) >/dev/null 2>&1 || true
	$(COMPOSE) up -d --build

stop: ## stop the module (package or source)
	$(COMPOSE) down
	@docker rm -f $(CONTAINER) >/dev/null 2>&1 || true

restart: stop run ## stop, rebuild and start again (after a code or .env change)

logs: ## follow the module's log (Ctrl-C to quit)
	docker logs -f $(CONTAINER)

status: ## show the module's container
	@docker ps -a --filter name=^$(CONTAINER)$$ --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'

## The module from the source (development), instead of the package
source-run: ## build and start the source version in Docker (make stop to stop)
	@docker rm -f $(CONTAINER) >/dev/null 2>&1 || true
	$(COMPOSE) up -d --build cedar-risk

python: ## run main.py directly with Python (ARGS="--pilots it")
	$(PYTHON) main.py $(ARGS)

## Local Kafka broker (a replica of the CEDAR cluster, on localhost:8004)
kafka-credentials: ## create the local credentials (once)
	./kafka/make-local-credentials.sh

kafka-up: ## start the local broker and create the topics
	$(KAFKA) up -d

kafka-down: ## stop the local broker
	$(KAFKA) down

kafka-logs: ## follow the local broker's log
	$(KAFKA) logs -f kafka

request: ## send a test request and wait for the answer (ARGS="--pilots sl --tender ...")
	$(PYTHON) kafka/send_request.py $(ARGS)

results: ## print a results topic of the local broker (ARGS=it_results_available)
	docker exec -it kafka-local /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:9094 \
	  --consumer.config /etc/kafka/secrets/admin-client.properties \
	  --topic $(or $(ARGS),sl_results_available) --from-beginning

## Synthetic anomaly scenarios (local Neo4j, no Kafka)
synthetic-up: ## start Neo4j + gateway and load the synthetic graph
	$(SYNTHETIC) up -d --build

synthetic-down: ## stop them
	$(SYNTHETIC) down

synthetic-test: ## score the synthetic graph; responses + ground truth in outputs/synthetic/
	$(PYTHON) synthetic_anomalies/test_synthetic.py $(ARGS)
