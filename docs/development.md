# Development

Use [native or Docker setup](setup.md) to run the application. The checks below
need no live ClickHouse server, browser login or private configuration. The
default tests use fixtures and block external network access.

## Native checks

Install Python 3.13 and uv, then run from the checkout root:

```sh
uv sync --frozen
uv run pytest
./run-linters.sh
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

## Run edited application code

**Native**

```sh
uv run web --no-reload
```

Stop the viewer with Ctrl+C and rerun it after editing code.

**Docker**

```sh
make up
```

This rebuilds the application image and recreates the service when needed.

## Parallel instances

Use a separate config, database and data directory for each native instance.
Select its config and viewer port explicitly:

```sh
uv run web --config task-config.toml --no-reload --port 8081
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
checkout keeps its own ignored `config.toml`.
