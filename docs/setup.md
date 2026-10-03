# Native and Docker setup

Choose a mode for each checkout. Both read `./config.toml`; their connection
and filesystem settings refer to different environments.

| Setting | Native | Docker Compose |
| --- | --- | --- |
| Template | `config.native.example.toml` | `config.example.toml` |
| ClickHouse host | Your server, usually `localhost` | `clickhouse` |
| Relative `data_dir = "data"` | Host working directory | `/app/data` volume |
| Browser | Installed host browser | Bundled Chromium launcher |
| Schema setup | Initialise your server once | Automatic on first startup |

The [Make command reference](../README.md#make-commands) lists the shortcuts.
`MODE` selects the launcher and the template used when configuration is missing;
it never converts an existing configuration.

Existing native users can keep their config, server and data. Templates are
starting points for new setups; they do not migrate or rewrite an existing
configuration. Separate checkouts are simplest when using both modes.

## Native

Install Python 3.13, uv and ClickHouse, and start your ClickHouse service.
The Make shortcuts additionally require `make`.
X collection needs an installed Chromium-based browser; media tools such as
FFmpeg must also be installed on the host when needed.

For a fresh checkout, create a private configuration without overwriting one:

```sh
uv sync --frozen
make config MODE=native
```

Edit `config.toml` for your server credentials, sources and data directory.
The example uses ClickHouse's HTTP interface on port 8123. Leave
`browser.executable_path` unset for auto-detection, or set a host browser path.

For a new database, initialise tables once using your ClickHouse client's
connection/authentication options. With a local default connection:

```sh
clickhouse client --multiquery < schema.sql
```

`schema.sql` creates the `dank` database and uses `IF NOT EXISTS`. Existing
installations can continue using their tables. Start the native viewer with
`make web MODE=native`, shown below. Stop it with Ctrl+C; restart after
configuration changes.

## Docker

Start Docker and run `make up` from the checkout containing `compose.yaml`.
It builds the image, starts ClickHouse, initialises tables and waits for the
viewer. `make config` can create configuration without starting services.

The generated `config.toml` is private and ignored by Git. It is mounted
read-only at `/run/dank/config.toml`, then copied for the unprivileged `dank`
user. Existing configuration is never replaced. The Docker template's database
host and browser path are container-specific.

After editing configuration, reload the viewer with
`make web ARGS='--force-recreate dank'`. The image includes Python,
Chromium, Node and FFmpeg. Chromium's own sandbox is disabled inside Docker;
the application runs as the unprivileged `dank` user. Use `--headless` to scrape.

## Start, preload and query

Complete your chosen setup above, then use the corresponding commands.

**Native**

```sh
make web MODE=native
# In another terminal:
make download-model MODE=native
make query MODE=native ARGS='-q "SELECT count() FROM posts FINAL"'
```

**Docker**

```sh
make up
make download-model
make query ARGS='-q "SELECT count() FROM posts FINAL"'
```

Both viewers use [localhost:8080](http://127.0.0.1:8080) by default. For another
port, pass `ARGS='--port 8081'` to `make web MODE=native`, or change the Compose
mapping to `127.0.0.1:8081:8080`. Compose keeps its ClickHouse service on its private
network; it does not expose that database for native commands by default.

## Data and logs

Native assets, profile and scrape/process logs use the configured host paths.
The examples put logs at `data/dank.log`; existing configurations may differ.
Model downloads use the host's normal model cache. Stop the native viewer
with Ctrl+C and manage your ClickHouse service separately.

Docker's default project is `dank`, with named volumes `dank_clickhouse_data`,
`dank_dank_data` and `dank_model_cache`. These hold the database, assets/profile/
logs, and models respectively. `make logs` shows service logs; scrape/process
logs also live at `/app/data/dank.log` in the application container.
`make down` preserves these volumes. Removing volumes deletes their data.

Future schema changes need migration steps in either mode. New worktrees need
their own ignored configuration. See [development](development.md) for checks
and running multiple instances.
