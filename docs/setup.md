# Native and Docker setup

Choose a mode for each checkout. Both read `./config.toml`; their connection
and filesystem settings refer to different environments.

| Setting | Native | Docker Compose |
| --- | --- | --- |
| Template | `config.native.example.toml` | `config.example.toml` |
| ClickHouse host | Your server, usually `localhost` | `clickhouse` |
| Postgres host | Your server, usually `localhost` | `postgres` |
| Postgres password | `postgres-password`, matching your role | `postgres-password`, generated |
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
The Make shortcuts additionally require `make` and Bash.
X collection needs an installed Chromium-based browser; media tools such as
FFmpeg must also be installed on the host when needed. YouTube downloads need
Node 22+ or a supported Deno runtime on `PATH`; `uv sync --frozen` installs the
matching JavaScript solver package. See [media downloads](features/media.md).

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

### Postgres on your own server

The web app brings the Postgres schema up to date whenever it starts, so it
needs a server and [Atlas](#atlas). No feature stores anything there yet, so you
can run without both for now by turning it off in `config.toml`:

```toml
[postgres]
enabled = false
```

See [which data lives where](features/database.md#where-data-lives). Docker runs
Postgres 18, so use that major version or a newer one.

`make config MODE=native` creates `postgres-password`, a private file ignored
by Git that holds a random password, and leaves an existing `config.toml` alone.
Create a role that uses it, pasting the file's contents when prompted, and a
database, connected as your server's administrator:

```sh
cat postgres-password
createuser --pwprompt dank
createdb --owner=dank dank
```

`createuser` hashes the password before sending it, so it can't reach your
server's logs. To use an existing role, replace the file's contents with that
role's password.

Point `config.toml` at your server. Configurations from before Postgres have no
`[postgres]` table and default to the Compose service, so copy the table from
`config.native.example.toml`. `password_file` is relative to the directory where
you run DANK. Check that the file and the role match:

```sh
PGPASSWORD="$(cat postgres-password)" psql --host=localhost --username=dank --dbname=dank --command='SELECT 1'
```

### Atlas

[Atlas](https://atlasgo.io/) applies the Postgres migrations. It is a separate
program that `uv sync` can't install, so install it once per checkout:

```sh
make atlas
```

This downloads a pinned version into `.venv/bin` and checks it against a
checksum in the repository. DANK turns off Atlas's telemetry and update checks
whenever it runs it, so run Atlas through `make db-atlas`; see
[Postgres migrations](development.md#postgres-migrations). The binary is free to
use under [Atlas's own terms](https://atlasgo.io/community-edition) rather than
an open source license. The Docker image already includes it.

## Docker

The Make shortcuts require `make` and Bash on the host.
Start Docker and run `make up` from the checkout containing `compose.yaml`.
It builds the image, starts ClickHouse and Postgres, initialises tables and
waits for the viewer. `make config` can create configuration without starting
services.

The generated `config.toml` is private and ignored by Git. It is mounted
read-only at `/run/dank/config.toml`, then copied for the unprivileged `dank`
user. Existing configuration is never replaced. The Docker template's database
host and browser path are container-specific.

`make config` also creates `postgres-password`, a private file ignored by Git
that holds a random password, even when `config.toml` already exists. The
built-in `[postgres]` settings match the Compose service, so an existing Docker
install upgrades with `make up` alone. Compose gives the file to both containers
as a secret, and the application container copies it to
`/app/postgres-password` for the `dank` user. The viewer brings the Postgres
schema up to date whenever it starts.

Postgres reads the file only when it initialises an empty volume, so keep it. If
you lose it, `make config` creates a new password that doesn't match the
database. To change the password, set a new one, save it in `postgres-password`
and reload the viewer:

```sh
docker compose exec postgres psql --username=dank --dbname=dank --command='\password dank'
make web ARGS='--force-recreate dank'
```

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
mapping to `127.0.0.1:8081:8080`. Compose keeps its ClickHouse and Postgres
services on its private network; it does not expose those databases for native
commands by default.

## Data and logs

Native assets, profile and scrape/process logs use the configured host paths.
The examples put logs at `data/dank.log`; existing configurations may differ.
Model downloads use the host's normal model cache. Stop the native viewer
with Ctrl+C and manage your ClickHouse and Postgres services separately.

Docker's default project is `dank`, with named volumes `dank_clickhouse_data`,
`dank_postgres_data`, `dank_dank_data` and `dank_model_cache`. These hold the
two databases, assets/profile/logs, and models respectively. `make logs` shows
service logs; scrape/process logs also live at `/app/data/dank.log` in the
application container. `make down` preserves these volumes. Removing volumes
deletes their data.

## Back up and upgrade Postgres

**Docker**

With the stack running, save the database to a private file named
`backups/postgres-<UTC time>.dump`, which Git ignores:

```sh
make postgres-dump
```

A failed dump leaves only a `.partial` file, which you can delete.

To restore one, stop the viewer so nothing writes during the restore:

```sh
docker compose stop dank
make postgres-restore FILE=backups/postgres-20261103T120000Z.dump
make up
```

The restore replaces what the dump contains, in one transaction, so a failed
restore changes nothing. Tables created after the dump stay in place; for an
exact copy, restore into an empty volume as shown next.

Major versions change only deliberately, by dump and restore, because a new
major version can't read the old one's data files. Pinning the major version in
`compose.yaml` keeps this from happening by accident:

```sh
make postgres-dump
make down
docker volume rm dank_postgres_data
# Edit compose.yaml: change the postgres image to the new major version.
make up
make postgres-restore FILE=backups/postgres-20261103T120000Z.dump
```

Run `docker compose pull postgres` before `make up` to get a new minor version;
the data files stay compatible.

**Native**

Use your server's tools with the connection settings in `config.toml`.
`pg_dump` refuses a server newer than itself, so install client tools of at
least your server's major version:

```sh
pg_dump --host=localhost --username=dank --format=custom --file=dank.dump dank
pg_restore --host=localhost --username=dank --dbname=dank --clean --if-exists --no-owner --single-transaction dank.dump
```

Future schema changes need migration steps in either mode. New worktrees need
their own ignored configuration. See [development](development.md) for checks
and running multiple instances.
