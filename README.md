# DANK - Dense Analysis Network Knowledge

DANK is a locally hosted Dense Analysis tool for collecting and exploring
public Internet content. It collects RSS/Atom articles and X posts, preserves
raw source payloads, downloads media, and prepares posts for browsing and
semantic search using local text embeddings.

The broader goal is to automate contextual understanding of trends, public
perception and evolving narratives.

## Features

- [RSS and Atom feeds](docs/features/rss.md) — discover feeds and collect articles.
- [X accounts](docs/features/x.md) — capture posts from configured accounts.
- [Media downloads](docs/features/media.md) — download and store discovered assets.
- [Processing and embeddings](docs/features/processing.md) — prepare searchable posts.
- [Browsing and search](docs/features/search.md) — explore posts in the web viewer.
- [DANK reader](reader/README.md) — the new reader with saved feeds and bookmarks.
- [Scrape history](docs/features/history.md) — compare run timings, outcomes and resource limits.
- [Database queries](docs/features/database.md) — inspect collected and processed data.

## Choose how to run

DANK supports **native execution with uv** and **Docker Compose**. Both use
`config.toml` in the checkout root. Existing native users can keep their
ClickHouse connection, configuration and data directory.

### Native

Install Python 3.13, uv and ClickHouse; X collection also needs a Chromium-based
browser. Follow [native setup](docs/setup.md#native) to create configuration
and initialise a new database, then run:

```sh
uv sync --frozen
make web MODE=native
```

### Docker

Install Docker with Compose and `make`, and start your Docker engine:

```sh
make up
```

This creates missing configuration from `config.example.toml`, builds the
application, starts ClickHouse, initialises its tables and starts the viewer.
Python, Chromium and media tools are included in the image.

Open [localhost:8080](http://127.0.0.1:8080) for either mode. The supplied
configuration templates start with an empty source list.

## Choose sources and collect

Edit the top-level `sources` setting in your private `config.toml`, for example:

```toml
sources = ["blog.codinghorror.com"]
```

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
`feed_urls` and `tags`. `[rss].max_entries_per_feed` limits each feed per run;
omitting it preserves unlimited fetching. See [RSS configuration](docs/features/rss.md#configure).
`[media].download_types` selects which media to download; see
[media controls](docs/features/media.md#choose-downloads).

The [feature guides](#features) also show the equivalent direct commands.
Docker's `dank` is the service name; `run --rm` starts a one-off container and removes it on exit,
while named data volumes persist.

Scrape and process progress appears in the terminal. Normal messages go to
stdout; warnings and errors go to stderr. Both streams also go to the configured
log file. Pending stages report elapsed time every five seconds. See
[RSS progress](docs/features/rss.md#progress-and-errors) for the counts.
Sources run concurrently with shared per-host request limits and bounded queues.
See [concurrency settings](docs/features/history.md#concurrency) for defaults and tuning.

## Make commands

Make defaults to Docker. Add `MODE=native` to run an application command with
`uv` on the host. Use `make help` for a quick reference.

| Target | Action |
| --- | --- |
| `make up` or `make web` | Start the Docker stack; native mode runs the viewer in the foreground |
| `make scrape` | Collect configured sources; Docker adds `--headless` |
| `make process` | Process collected posts and assets |
| `make query` | Run `clickhouse-query` with arguments from `ARGS` |
| `make embed` | Run `embed-text` with text from `ARGS` |
| `make download-model` | Cache the embedding model |
| `make config` | Create missing configuration for the selected mode |
| `make down`, `make logs` | Stop or inspect the Docker stack; Docker mode only |

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
Docker. These copy the matching example template. Configuration is ignored
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

Native runs use your local ClickHouse and files. Docker stores database files,
assets, browser profile and model cache in named volumes. Use `make logs` and
`make down` to inspect and stop the Docker stack; `make down` preserves volumes.

See [setup](docs/setup.md) for both modes, or
[development](docs/development.md) for native and container checks.
