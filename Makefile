.DEFAULT_GOAL := up
MODE ?= docker
COMPOSE ?= docker compose
UV ?= uv
ARGS ?=

ifeq ($(MODE),docker)
CONFIG_TEMPLATE := config.example.toml
RUN := $(COMPOSE) run --rm -T dank
SCRAPE := $(RUN) scrape --headless
WEB := $(COMPOSE) up --build --wait
X_LOGIN := $(COMPOSE) run --build --rm --no-deps -T -p 127.0.0.1:6080:6080 dank dank-x-login
else ifeq ($(MODE),native)
CONFIG_TEMPLATE := config.native.example.toml
RUN := $(UV) run
SCRAPE := $(RUN) scrape
WEB := $(RUN) web --no-reload
X_LOGIN := $(RUN) x-login
else
$(error MODE must be docker or native)
endif

.PHONY: help config up web down logs scrape process query embed download-model x-login

help:
	@printf '%s\n' \
		'Usage: make <target> [MODE=docker|native] [ARGS=...]' \
		'Docker is the default. Native mode uses uv on the host.' \
		'' \
		'  up, web         Start the viewer (Docker stack or native foreground)' \
		'  scrape          Collect sources; save output under logs/scrape/' \
		'  x-login         Sign in to X and save the session for collection' \
		'  process         Process posts/assets; save output under logs/process/' \
		'  query           Run clickhouse-query; pass -q and SQL through ARGS' \
		'  embed           Run embed-text; pass quoted text through ARGS' \
		'  download-model  Cache an embedding model' \
		'  config          Create missing config.toml for the selected mode' \
		'  down, logs      Stop or view logs for the Docker stack' \
		'' \
		'ARGS goes to the application command, or to Compose for Docker up/web.' \
		'Existing configuration is preserved; MODE does not convert it.'

config:
	@set -e; if [ ! -e config.toml ]; then \
		(umask 077; cp -n $(CONFIG_TEMPLATE) config.toml); \
		printf '%s\n' 'Created config.toml for $(MODE). Edit sources there before scraping.'; \
	fi

# Preserve literal dollar signs in arguments, including regex end anchors.
up web: config
	$(WEB) $(value ARGS)

scrape: config
	@bash src/dank/run_logged.sh scrape $(MODE) $(SCRAPE) $(value ARGS)

x-login: config
	$(X_LOGIN) $(value ARGS)

process: config
	@bash src/dank/run_logged.sh process $(MODE) $(RUN) process $(value ARGS)

query: config
	$(RUN) clickhouse-query $(value ARGS)

embed: config
	$(RUN) embed-text $(value ARGS)

download-model: config
	$(RUN) download-embedding-model $(value ARGS)

ifeq ($(MODE),docker)
down:
	$(COMPOSE) down $(value ARGS)

logs:
	$(COMPOSE) logs --follow $(value ARGS)
else
down logs:
	@printf '%s\n' '$@ manages Docker services; use MODE=docker.' >&2
	@printf '%s\n' 'For native runs, stop the viewer with Ctrl+C and use its configured log file.' >&2
	@exit 2
endif
