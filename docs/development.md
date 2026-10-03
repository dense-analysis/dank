# Development

Use the [container setup](setup.md) to run DANK and ClickHouse. For editing and
running unit tests on the host, install Python 3.13 and uv:

```sh
uv sync --frozen
uv run pytest
./run-linters.sh
uv run pyright
```

The default tests use fixtures and block external network access. They need no
ClickHouse server, browser login or private configuration. Real-model embedding
checks are separate and require the models to be available:

```sh
uv run pytest -m embeddings -s
```

The linter script can fix files; inspect your diff afterward. Running Pyright
directly also makes its exit status visible.

## Containers during development

Application code is copied into the image. Rebuild after changing it:

```sh
make up
```

Feature-guide examples run commands inside the application image. The matching
host commands use `uv run` instead of `docker compose run --rm dank`; host
execution needs a configuration and database connection appropriate to the
host. The generated container configuration uses the internal hostname
`clickhouse`.

For parallel checkouts, give each stack a distinct project name and host port.
Use a local Compose override to change the viewer's port, then consistently
supply the same project and files for all commands:

```sh
make up COMPOSE='docker compose -p dank-my-task -f compose.yaml -f compose.local.yaml'
docker compose -p dank-my-task -f compose.yaml -f compose.local.yaml ps
```

`compose.local.yaml` example (requires Compose 2.24.4 or later):

```yaml
services:
  dank:
    ports: !override
      - "127.0.0.1:8081:8080"
```

Each project gets separate database, data and model-cache volumes. Each
checkout also keeps its own ignored `config.toml`.
