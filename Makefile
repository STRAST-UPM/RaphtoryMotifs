# CEDAR risk module: run, stop and restart it (the installed package).
# The broker and every other setting come from .env -- for production:
# KAFKA_BOOTSTRAP_SERVERS=kafka.cedar-heu-project.eu:8004 (see .env.example).
#
#   make run        build and start the module
#   make stop       stop it
#   make restart    stop it, rebuild and start it again (after a code or .env change)

COMPOSE   ?= docker compose
CONTAINER := cedar-risk

.PHONY: run stop restart

run:
	@echo "broker: $$(grep -E '^KAFKA_BOOTSTRAP_SERVERS=' .env | cut -d= -f2-)"
	@docker rm -f $(CONTAINER) >/dev/null 2>&1 || true
	$(COMPOSE) up -d --build

stop:
	$(COMPOSE) down
	@docker rm -f $(CONTAINER) >/dev/null 2>&1 || true

restart: stop run
