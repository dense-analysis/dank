# DANK - Dense Analysis Network Knowledge

DANK is a locally hosted Dense Analysis tool for collecting and exploring
public Internet content. It collects RSS/Atom articles, X posts, Reddit
community posts, Hacker News stories and YouTube feeds, preserves raw source payloads, downloads
media, and prepares posts for browsing and semantic search using local text
embeddings.

The broader goal is to automate contextual understanding of trends, public
perception and evolving narratives.

## Features

- [RSS and Atom feeds](docs/features/rss.md) — discover feeds and collect articles.
- [X feeds](docs/features/x.md) — save For You recommendations and account posts.
- [Reddit communities](docs/features/reddit.md) — collect ranked community posts.
- [Hacker News](docs/features/hacker-news.md) — browse daily front-page stories and optional discussions.
- [YouTube feeds](docs/features/youtube.md) — save recommendations and their context.
- [Media downloads](docs/features/media.md) — download and store discovered assets.
- [Processing and embeddings](docs/features/processing.md) — prepare searchable posts.
- [Collection provenance](docs/features/collection-provenance.md) — trace saved articles, discovery order and contextual connections.
- [Browsing and search](docs/features/search.md) — explore posts in the web viewer.
- [DANK reader](reader/README.md) — read collected stories with search, source filters and reading preferences.
- [Scrape history](docs/features/history.md) — compare run timings, outcomes and resource limits.
- [Stored data and queries](docs/features/database.md) — see which data lives where and inspect it.
- [Sources in Postgres](docs/features/sources.md) — import sources once, then use them without restarting DANK.

See [known collection and processing issues](docs/known-issues.md) for reader
status meanings, source-specific problems, current handling and recovery gaps.
Track outstanding work and future ideas in the [DANK project in Linear](https://linear.app/dense-analysis/project/dank-de00db7ebbb2).

## Codex Configuration

For configuring OpenAI Codex with Docker Sandboxes, you should run the
following:

```sh
# Set up Codex for the project with playwright for browser testing.
sbx run docker.io/docker/sbx-kit-codex:0.155.1 \
    --name codex-dank \
    --kit docker.io/sbx/playwright:latest

# Allow fetching PyTorch modules
sbx policy allow network --sandbox codex-dank download.pytorch.org,download-r2.pytorch.org

# Allow all HTTP get requests from Codex.
sbx policy allow network --sandbox codex-dank '**' --method GET,HEAD

# Install socat and a script for giving the sandbox access to our ClickHouse
sbx policy allow network --sandbox codex-dank localhost:8123
sbx exec -u root codex-dank bash -c 'apt-get update && apt-get install -y socat'
sbx exec codex-dank bash -c 'grep -Fq "TCP-LISTEN:8123" /etc/sandbox-persistent.sh || echo "if ! pgrep -x socat >/dev/null; then nohup socat TCP-LISTEN:8123,bind=127.0.0.1,reuseaddr,fork TCP:host.docker.internal:8123 >/tmp/clickhouse-forward.log 2>&1 </dev/null & fi" >> /etc/sandbox-persistent.sh; source /etc/sandbox-persistent.sh'
sbx stop codex-dank

# Set up the codex instance to run `uv run` with a different .venv.
# This stops the sandbox interfering with your .venv directory.
sbx exec -u root codex-dank bash -c 'printf "%s\n" "export UV_PROJECT_ENVIRONMENT=/home/agent/.venvs/dank" >> /etc/sandbox-persistent.sh'
sbx stop codex-dank

# Allow connecting to Linear.
sbx policy allow network --sandbox codex-dank mcp.linear.app
sbx run codex -- mcp add linear --url https://mcp.linear.app/mcp
sbx run codex -- mcp login linear
```

## Choose how to run

DANK supports **native execution with uv** and **Docker Compose**. Both use
`config.toml` in the checkout root. Existing native users can keep their
ClickHouse connection, configuration and data directory.

### Native

Install Python 3.13, uv and ClickHouse; X and Reddit collection also need a
Chromium-based browser. Follow [native setup](docs/setup.md#native) to create
configuration and initialise a new database, then run:

```sh
uv sync --frozen
make web MODE=native
```

### Docker

Install Docker with Compose, `make` and Bash, then start your Docker engine:

```sh
make up
```

This creates missing configuration from `config.example.toml`, builds the
application, starts ClickHouse and Postgres, initialises ClickHouse's tables and
starts the viewer. Python, Chromium and media tools are included in the image.

Open [localhost:8080](http://127.0.0.1:8080) for either mode. The supplied
configuration templates start with an empty source list.

## Choose sources and collect

Edit the top-level `sources` setting in your private `config.toml`, for example:

```toml
sources = ["blog.codinghorror.com"]
```

`sources` is deprecated in favour of [sources in Postgres](docs/features/sources.md),
which apply edits without a restart. Import them once with
`make sources-import`, then set `sources_in_database = true` near the top of
`config.toml`, before any `[section]`. The example configs already have the
line set to `false`. From then on, DANK no longer uses `sources` in
`config.toml`.

**Native**

```sh
make scrape MODE=native
make process MODE=native ARGS='--age 24h'
```

**Docker**

```sh
make scrape
make process ARGS='--age 24h'
```

Source arrays can mix domain strings with tables containing optional
`feed_urls`, `tags` and `adapters`. A source can compose collection methods,
including [Hacker News with RSS](docs/features/hacker-news.md#add-rss-or-use-it-alone). `[rss].max_entries_per_feed` limits each feed per run;
omitting it preserves unlimited fetching. See [RSS configuration](docs/features/rss.md#configure).
Reddit sources use `feed_urls` for community URLs or `r/community` shorthand;
see [Reddit configuration](docs/features/reddit.md#configure).
`[media].download_types` selects which media to download; see
[media controls](docs/features/media.md#choose-downloads).

The [feature guides](#features) also show the equivalent direct commands.
Docker's `dank` is the service name; `run --rm` starts a one-off container and removes it on exit,
while named data volumes persist.

Scrape and process progress appears in the terminal. Normal messages go to
stdout; warnings and errors go to stderr. Both streams also go to the configured
log file. Pending stages report elapsed time every five seconds. See
[RSS progress](docs/features/rss.md#progress-and-errors) for the counts.
Sources run concurrently with shared per-host request limits. Posts are saved
during discovery, while separate workers fetch assets without pausing navigation.
See [concurrency settings](docs/features/history.md#concurrency) for defaults and tuning.

Every `make scrape` and `make process` also saves a timestamped terminal log
under `logs/scrape/` or `logs/process/` in your checkout, in both modes.
Logs include the command, checkout revision, duration and exit status, and
are kept for later comparisons. See [run logs](docs/features/history.md#run-logs).

## Make commands

Make defaults to Docker. Add `MODE=native` to run an application command with
`uv` on the host. Use `make help` for a quick reference.

| Target | Action |
| --- | --- |
| `make up` or `make web` | Start the Docker stack; native mode runs the viewer in the foreground |
| `make x-login` | Sign in to X and save the session for collection |
| `make youtube-login` | Save an optional, separate YouTube session |
| `make youtube-compare` | Compare browser and yt-dlp recommendations |
| `make scrape` | Collect configured sources; Docker adds `--headless` |
| `make process` | Process collected posts and assets |
| `make train` | Label category training examples with Decisions; see [categories](docs/features/categories.md) |
| `make query` | Run `clickhouse-query` with arguments from `ARGS` |
| `make embed` | Run `embed-text` with text from `ARGS` |
| `make download-model` | Cache the embedding model |
| `make config` | Create missing configuration for the selected mode, and `postgres-password` |
| `make down`, `make logs` | Stop or inspect the Docker stack; Docker mode only |
| `make postgres-dump` | Save the Docker Postgres database under `backups/`; Docker mode only |
| `make postgres-restore FILE=<dump>` | Restore a saved dump into it; Docker mode only |
| `make atlas` | Install the pinned Atlas binary for native runs |
| `make db-upgrade`, `make db-current`, `make db-create` | Manage the Postgres schema; see [Postgres migrations](docs/development.md#postgres-migrations) |
| `make db-diff NAME=<name>`, `make db-check` | Generate a migration from model changes, or check they match; on the host |
| `make db-atlas ARGS='<command>'` | Run any other Atlas command with telemetry off; on the host |
| `make sources-import` | Import the sources in `config.toml` into Postgres, once; see [sources](docs/features/sources.md) |
| `make test` | Run the tests; pass paths or options through `ARGS`; on the host |
| `make test-postgres` | Run the Postgres tests against a disposable server |
| `make lint` | Run all linters, which can fix files; on the host |

**Native**

```sh
make query MODE=native ARGS='-q "SELECT count() FROM posts FINAL"'
make scrape MODE=native ARGS='--headless --domains "^nichegamer\.com$"'
make embed MODE=native ARGS='"A phrase to embed"'
```

**Docker**

```sh
make query ARGS='-q "SELECT count() FROM posts FINAL"'
make scrape ARGS='--domains "^nichegamer\.com$"'
make embed ARGS='"A phrase to embed"'
```

`ARGS` uses shell quoting and goes to the underlying application command.
For Docker `up`/`web`, it goes to `docker compose up` instead. Native `web`
disables hot reload by default. `COMPOSE` and `UV` can override the launchers.

## Configuration and storage

Use `make config MODE=native` for a new native setup or `make config` for
Docker. These copy the matching example template and create a private
`postgres-password` file with a random password. Both are ignored
by Git. Existing files are preserved, but switching modes requires the matching database host,
browser path and data location; copying a template does not migrate data.

After configuration edits, stop the native viewer with Ctrl+C, then restart
it using the matching command.

**Native**

```sh
make web MODE=native
```

**Docker**

```sh
make web ARGS='--force-recreate dank'
```

Native runs use your local ClickHouse and files, and
[your own Postgres server](docs/setup.md#postgres-on-your-own-server) unless you
turn it off. Docker
stores database files, assets, browser profile and model cache in named volumes.
Use `make logs` and `make down` to inspect and stop the Docker stack; `make down`
preserves volumes.

See [setup](docs/setup.md) for both modes, or
[development](docs/development.md) for native and container checks.
