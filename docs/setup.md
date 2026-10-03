# Container setup

DANK runs as two Compose services: `clickhouse` stores records, and `dank`
serves the web viewer and runs collection/processing commands. Start Docker,
then run `make up` from the checkout containing `compose.yaml`.

## Where configuration lives

`make config` copies `config.example.toml` to **`./config.toml`** with private
file permissions, only if the file is missing. `make up` includes this step.
Edit this file to choose sources, login details and collection settings.

The file is mounted read-only at `/run/dank/config.toml` in containers. The
entry point creates a private application-readable copy, then runs the command
as the unprivileged `dank` user. Recreate the viewer after edits:

```sh
docker compose up -d --force-recreate dank
```

Each `docker compose run --rm dank ...` command starts a fresh container and
loads the current configuration. New Git worktrees need their own
`config.toml`; ignored files are not copied between checkouts.

The example's ClickHouse hostname is `clickhouse`, the Compose service name.
An existing configuration is never rewritten automatically: if you previously
ran on the host, compare its connection, storage and browser settings with
`config.example.toml` before using the containers.

## What each setup file does

- `Dockerfile` builds Python, locked dependencies, Chromium, Node and FFmpeg
  together with DANK. Its default command starts the web viewer.
- `compose.yaml` starts ClickHouse, mounts `schema.sql` for initial table
  creation, and waits for the database before starting the viewer.
- `Makefile` creates missing configuration and wraps starting, stopping and logs.
- `.dockerignore` excludes private configuration and runtime files from builds.

The container uses a Chromium wrapper that disables Chromium's own sandbox;
the application still runs as the unprivileged `dank` user within Docker.
Use `--headless` for browser collection in this image.

Database creation is automatic on first startup. The initialisation SQL uses
`IF NOT EXISTS`; future schema changes still need their own migration steps.

## Persistent data

The default Compose project is `dank`. Its named volumes are:

| Volume | Contents |
| --- | --- |
| `dank_clickhouse_data` | ClickHouse database files |
| `dank_dank_data` | Assets, browser profile and `dank.log` |
| `dank_model_cache` | Downloaded embedding models and library caches |

In the application container, the default `data_dir = "data"` resolves to
`/app/data`. Keep this path to use the mounted volume. Ordinary `make down`
preserves volumes; removing the volumes deletes their stored data.

## Useful commands

```sh
docker compose run --rm dank download-embedding-model
docker compose run --rm dank clickhouse-query -q 'SELECT count() FROM posts FINAL'
docker compose logs --tail 100 clickhouse dank
```

Model files download on demand when processing or searching non-empty text;
the first command above preloads the default model. Initial startup with the
empty source list does not scrape sites or require an embedding model.

If port 8080 is busy, change the host side of the viewer's port mapping in
`compose.yaml`, for example `127.0.0.1:8081:8080`. Keep ClickHouse on the private
Compose network. Scrape and process logs also persist at `/app/data/dank.log` inside the
application container. See [development](development.md) for isolated worktrees.
