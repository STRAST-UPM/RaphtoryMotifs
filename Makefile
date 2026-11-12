# CEDAR risk module: run, stop and restart it.
# The broker and every other setting come from .env -- for production:
# KAFKA_BOOTSTRAP_SERVERS=kafka.cedar-heu-project.eu:8004 (see .env.example).
#
#   make run                      build and start the module from the source (default)
#   make run PROFILE=package      build and start the installed package instead
#   make stop                     stop it (whichever runs)
#   make restart [PROFILE=...]    stop it, rebuild and start it again (after a code or .env change)
#
# Only one runs at a time: both use the container name cedar-risk, and `run`
# removes the existing one first.

COMPOSE   ?= docker compose
CONTAINER := cedar-risk
PROFILE   ?=

ifeq ($(PROFILE),)
SERVICE := cedar-risk
else ifeq ($(PROFILE),package)
SERVICE := cedar-risk-package
else
$(error PROFILE must be empty (the source) or package, not "$(PROFILE)")
endif

.PHONY: run stop restart

run:
	@echo "broker : $$(grep -E '^KAFKA_BOOTSTRAP_SERVERS=' .env | cut -d= -f2-)"
	@echo "running: $(SERVICE)"
	@docker rm -f $(CONTAINER) >/dev/null 2>&1 || true
	$(COMPOSE) up -d --build $(SERVICE)

stop:
	@docker rm -f $(CONTAINER) >/dev/null 2>&1 || true
	$(COMPOSE) down

restart: stop run
