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
uv run web --no-reload
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
uv run scrape --headless
uv run process --age 24h
```

**Docker**

```sh
docker compose run --rm dank scrape --headless
docker compose run --rm dank process --age 24h
```

The [feature guides](#features) show both command forms. Docker's `dank` is
the service name; `run --rm` starts a one-off container and removes it on exit,
while named data volumes persist.

## Configuration and storage

Use `config.native.example.toml` for a new native setup and
`config.example.toml` for Docker. Configuration is ignored by Git. Existing
files are preserved, but switching modes requires the matching database host,
browser path and data location; copying a template does not migrate data.

After configuration edits, stop the native viewer with Ctrl+C, then restart
it using the matching command.

**Native**

```sh
uv run web --no-reload
```

**Docker**

```sh
docker compose up -d --force-recreate dank
```

Native runs use your local ClickHouse and files. Docker stores database files,
assets, browser profile and model cache in named volumes. Use `make logs` and
`make down` to inspect and stop the Docker stack; `make down` preserves volumes.

See [setup](docs/setup.md) for both modes, or
[development](docs/development.md) for native and container checks.
