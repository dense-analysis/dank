.DEFAULT_GOAL := up
COMPOSE ?= docker compose

.PHONY: config up down logs

config:
	@set -e; if [ ! -e config.toml ]; then \
		(umask 077; cp -n config.example.toml config.toml); \
		printf '%s\n' 'Created config.toml. Edit sources there before scraping.'; \
	fi

up: config
	$(COMPOSE) up --build --wait

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs --follow
