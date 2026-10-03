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

## Quick start

Install Docker with Docker Compose and `make`, and start your Docker engine
(Docker Desktop or OrbStack on macOS). From this checkout, run:

```sh
make up
```

This creates **`config.toml` in this directory** from
[`config.example.toml`](config.example.toml) if it is missing, builds the
application image, starts ClickHouse, creates the database tables, and starts
the web viewer at [localhost:8080](http://127.0.0.1:8080).

Containers provide Python, Chromium and the other runtime dependencies. The
first build downloads them; later starts reuse the image and volumes.

## Choose sources and collect

Edit the generated `config.toml`. It starts with an empty source list. For
example, change its top-level `sources` setting to:

```toml
sources = ["blog.codinghorror.com"]
```

Then collect and process content:

```sh
docker compose run --rm dank scrape --headless
docker compose run --rm dank process --age 24h
```

The [feature guides](#features) cover source options and other commands. These
commands use the same configuration and persistent storage. To collect
only selected configured domains, add `--domains` to `scrape`.

`config.toml` stays private and is ignored by Git. `make config` creates it
without starting containers, and never replaces an existing file. For X,
add your account settings as described in [X collection](docs/features/x.md).

After editing configuration, recreate the viewer to load it:

```sh
docker compose up -d --force-recreate dank
```

## Manage the stack

```sh
docker compose ps
make logs
make down
```

`make down` stops the stack while preserving the database, downloaded assets,
browser profile and model cache in Docker volumes. `make up` starts it again.
ClickHouse is available inside the Compose network; only the web viewer is
published on the host, at `127.0.0.1:8080`.

See [container setup](docs/setup.md) for configuration paths, storage and
troubleshooting, or [development](docs/development.md) for local tests and uv.
