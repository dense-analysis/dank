# Development

Use [native or Docker setup](setup.md) to run the application. The checks below
need no live ClickHouse or Postgres server, browser login or private
configuration. The default tests use fixtures and block external network access.

## Native checks

Install Python 3.13 and uv, then run from the checkout root:

```sh
uv sync --frozen
make test
make lint
uv run pyright
```

The linter can fix files; inspect the resulting diff. Running Pyright directly
also exposes its exit status.

## Docker checks

The runtime image omits test dependencies and test files. Build the current
source, mount the tests read-only, then install the locked development tools
inside a disposable container:

```sh
docker build -t dank-checks .
docker run --rm --entrypoint sh \
  --mount "type=bind,source=$PWD/tests,target=/app/tests,readonly" \
  --mount "type=bind,source=$PWD/run-linters.sh,target=/app/run-linters.sh,readonly" \
  dank-checks -c 'uv sync --frozen && uv run pytest && ./run-linters.sh && uv run pyright'
```

These checks operate on the source copied into the image. Linter fixes stay in
the disposable container; use the native command to apply fixes to the checkout.
Rebuild the image after changing application code. This command does not start
the Compose services or mount private configuration and application data.

Real-model checks are opt-in. Replace `uv run pytest` in either example with
`uv run pytest -m embeddings -s`; the models listed in the embedding tests must
already be cached. In Docker, make that cache available inside the check
container at `/root/.cache/huggingface`, for example through a read-only mount.

## FastAPI server

The JSON API is moving to FastAPI one route group at a time; see
[API conventions](api.md). Run the new server beside the default one:

```sh
uv run web --server fastapi --port 8081
```

It reloads on changes under `src/dank` through watchfiles, which also works on
macOS. Its ClickHouse adapter tests need a loopback ClickHouse and run with
`uv run pytest tests/api --clickhouse-url=http://default:@127.0.0.1:18123`;
without the option they are skipped.

## Run edited application code

**Native**

```sh
make web MODE=native
```

Stop the viewer with Ctrl+C and rerun it after editing code.

**Docker**

```sh
make up
```

This rebuilds the application image and recreates the service when needed.

## Postgres migrations

SQLAlchemy models in `src/dank/db/models/` describe the Postgres schema.
[Atlas](https://atlasgo.io/) generates SQL migrations from them into
`src/dank/db/migrations/`, and applies those. Atlas is a trial: only
`src/dank/db/atlas.py` runs it, so it stays easy to replace, and
[DEN-57](https://linear.app/dense-analysis/issue/DEN-57) decides whether it
stays. [Install it first](setup.md#atlas).

The web app upgrades to head when it starts, and two starting together migrate
once. Scrape and process commands that need Postgres call `require_head`: they
stop with a clear message unless the schema is at head, and never migrate. A
database with migrations this code doesn't know about is always refused, which
Atlas alone would report as fine.

| Command | Purpose |
| --- | --- |
| `make db-upgrade` | Apply pending migrations |
| `make db-current` | Show migrations; exits 1 unless the schema is at head |
| `make db-create` | Create the configured database if it's missing, for a new worktree |
| `make db-diff NAME=<name>` | Generate a migration from model changes |
| `make db-check` | Check migrations are valid and match the models |
| `make db-atlas ARGS='migrate hash --env dank'` | Run any other Atlas command, with telemetry off |

`db-upgrade`, `db-current` and `db-create` follow `MODE`; the others always run on
the host. `db-diff` and `db-check` compare schemas in a temporary Docker container.
Without Docker, pass your own empty database:
`uv run dank-db check --dev-url 'postgres://user:password@localhost:5432/atlas_dev?sslmode=prefer&search_path=public'`.

Rules for migrations:

- They only move forward, and released ones are never edited. Editing an
  unreleased one needs `migrate hash`, and `atlas.sum` refuses it otherwise.
- Review every generated migration, especially any `DROP`. Atlas asks about
  renames only in a terminal. Without one, as for an agent, it writes a drop and
  an add, which loses the column's data, so write those renames by hand.
- For a merge conflict in `atlas.sum`, keep either side and run `migrate hash`.
  If your migration is now older than one already applied, run
  `make db-atlas ARGS='migrate rebase <version> --env dank'`.
- Don't rely on Atlas Pro features such as `migrate lint` or `migrate test` unless
  DEN-57 decides to pay for them. Hand-written views, functions and triggers in a
  migration work on the free tier, though Atlas won't track them.

A preview of a branch with a migration that isn't on `main` must use a
Postgres database of its own. On the real database it would apply that
migration, and `main` would then refuse to start, correctly, because the
database is ahead of its code. A preview of an older branch refuses to start
for the same reason. Point the preview's config at another database, created
with `make db-create`, as for [parallel instances](#parallel-instances).

`make test-postgres` runs the tests marked `postgres` against a disposable server.
Without `--postgres-url` or Atlas they're skipped, so `uv run pytest` needs
neither. They get these fixtures from `tests/conftest.py`:

| Fixture | Gives a test |
| --- | --- |
| `postgres_session`, `async_postgres_session` | A session on a database at head, rolled back afterwards even after a commit |
| `postgres_settings` | Settings for that database |
| `fresh_postgres` | An empty database of its own, for migration tests |
| `postgres_dev_url` | An empty database for Atlas to compare schemas in |

## Parallel instances

Use a separate config, database and data directory for each native instance.
Select its config and viewer port explicitly:

```sh
make web MODE=native ARGS='--config task-config.toml --port 8081'
```

Worktrees sharing a Postgres server each use their own database, as they do on
a shared ClickHouse server, so a task's writes and schema changes can't reach
another's. `make config` gives each worktree a new random `postgres-password`,
which doesn't match the shared role, so point at the file that does:

```toml
[postgres]
host = "localhost"
database = "dank_my_task"
password_file = "../main/postgres-password"
```

Create the database once with `make db-create MODE=native` if the role may
create databases, or connected as your server's administrator:

```sh
createdb --owner=dank dank_my_task
```

For Docker, give each stack a distinct project name and host port. Create an
ignored `compose.local.yaml` override (Compose 2.24.4 or later):

```yaml
services:
  dank:
    ports: !override
      - "127.0.0.1:8081:8080"
```

Use that project and override consistently for its commands:

```sh
make up COMPOSE='docker compose -p dank-my-task -f compose.yaml -f compose.local.yaml'
docker compose -p dank-my-task -f compose.yaml -f compose.local.yaml ps
```

Each Compose project has separate database, data and model-cache volumes. Each
checkout keeps its own ignored `config.toml` and `postgres-password`.
