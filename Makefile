.DEFAULT_GOAL := up
MODE ?= docker
COMPOSE ?= docker compose
UV ?= uv
ARGS ?=
FILE ?=
# The Compose service's role and database.
POSTGRES_ARGS := --username=dank --dbname=dank

ifeq ($(MODE),docker)
CONFIG_TEMPLATE := config.example.toml
RUN := $(COMPOSE) run --rm -T dank
SCRAPE := $(RUN) scrape --headless
WEB := $(COMPOSE) up --build --wait
X_LOGIN := bash src/dank/run_x_login.sh $(COMPOSE) --
YOUTUBE_LOGIN := python3 src/dank/run_youtube_login.py $(COMPOSE) --
else ifeq ($(MODE),native)
CONFIG_TEMPLATE := config.native.example.toml
RUN := $(UV) run
SCRAPE := $(RUN) scrape
WEB := $(RUN) web --no-reload
X_LOGIN := $(RUN) x-login
YOUTUBE_LOGIN := $(RUN) youtube-login
else
$(error MODE must be docker or native)
endif

.PHONY: help config up web down logs scrape process query embed download-model x-login youtube-login youtube-compare postgres-dump postgres-restore

help:
	@printf '%s\n' \
		'Usage: make <target> [MODE=docker|native] [ARGS=...]' \
		'Docker is the default. Native mode uses uv on the host.' \
		'' \
		'  up, web         Start the viewer (Docker stack or native foreground)' \
		'  scrape          Collect sources; save output under logs/scrape/' \
		'  x-login         Sign in to X and save the session for collection' \
		'  youtube-login   Sign in to Google using 1Password' \
		'  youtube-compare Compare browser and yt-dlp recommendations' \
		'  process         Process posts/assets; save output under logs/process/' \
		'  query           Run clickhouse-query; pass -q and SQL through ARGS' \
		'  embed           Run embed-text; pass quoted text through ARGS' \
		'  download-model  Cache an embedding model' \
		'  config          Create missing config.toml and postgres-password' \
		'  down, logs      Stop or view logs for the Docker stack' \
		'  postgres-dump   Save the Docker Postgres database under backups/' \
		'  postgres-restore  Restore a dump into it; pass FILE=backups/<name>.dump' \
		'' \
		'ARGS goes to the application command, or to Compose for Docker up/web.' \
		'Existing configuration is preserved; MODE does not convert it.'

config:
	@set -e; if [ ! -e config.toml ]; then \
		(umask 077; cp -n $(CONFIG_TEMPLATE) config.toml); \
		printf '%s\n' 'Created config.toml for $(MODE). Edit sources there before scraping.'; \
	fi; \
	if [ ! -e postgres-password ]; then \
		(umask 077; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > postgres-password); \
		printf '%s\n' 'Created postgres-password with a random password.'; \
	fi

# Preserve literal dollar signs in arguments, including regex end anchors.
up web: config
	$(WEB) $(value ARGS)

scrape: config
	@bash src/dank/run_logged.sh scrape $(MODE) $(SCRAPE) $(value ARGS)

x-login: config
	$(X_LOGIN) $(value ARGS)

youtube-login: config
	$(YOUTUBE_LOGIN) $(value ARGS)

youtube-compare: config
	$(RUN) youtube-compare $(value ARGS)

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

# Write beside the final name so a failed dump can't pass for a backup.
postgres-dump:
	@set -e; umask 077; mkdir -p backups; \
	dump="backups/postgres-$$(date -u '+%Y%m%dT%H%M%SZ').dump"; \
	$(COMPOSE) exec -T postgres pg_dump $(POSTGRES_ARGS) --format=custom > "$$dump.partial"; \
	mv "$$dump.partial" "$$dump"; \
	printf 'Saved %s\n' "$$dump"

postgres-restore:
	@if [ -z '$(value FILE)' ]; then \
		printf '%s\n' 'Pass the dump to restore: make postgres-restore FILE=backups/<name>.dump' >&2; \
		exit 2; \
	fi
	$(COMPOSE) exec -T postgres pg_restore $(POSTGRES_ARGS) --clean --if-exists --no-owner --single-transaction < '$(value FILE)'
else
down logs:
	@printf '%s\n' '$@ manages Docker services; use MODE=docker.' >&2
	@printf '%s\n' 'For native runs, stop the viewer with Ctrl+C and use its configured log file.' >&2
	@exit 2

postgres-dump postgres-restore:
	@printf '%s\n' '$@ manages the Docker Postgres. For your own server, use pg_dump and pg_restore; see docs/setup.md.' >&2
	@exit 2
endif
