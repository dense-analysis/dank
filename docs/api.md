# API

DANK's JSON API is moving from aiohttp to FastAPI, one route group at a time.
The FastAPI app lives in `src/dank/api/`. The aiohttp server in
`src/dank/web/` stays the default until every route has moved.

```sh
uv run web --server fastapi          # reloads on changes under src/dank
uv run web --server fastapi --no-reload --port 8081
```

`web` keeps its other flags (`--config`, `--host`, `--port`, `--limit`,
`--no-reload`). Ported so far: `GET /api/reader/sources`. OpenAPI is served at
`/api/openapi.json`, with interactive docs at `/api/docs`.

## Shape

Every feature has three layers:

- **Domain** (`model.py`): DANK's rules as NamedTuples and pure functions.
- **Application** (`service.py`): a service class that runs the use cases,
  plus a `Protocol` for each outside thing it needs (its ports).
- **Edge and adapters**: the router and Pydantic schemas turn HTTP into service
  calls; adapters implement the ports against ClickHouse, Postgres or
  `config.toml`.

Dependencies point inward: the edge and adapters import the service and model,
never the other way round.

```text
src/dank/
  errors.py              DankError, InvalidInput, NotFound, Conflict
  api/
    app.py               create_app(): routers, error handlers, guard, headers
    lifespan.py          Resources, opened at startup and closed at shutdown
    deps.py              ResourcesDep, SettingsDep
    errors.py            the error envelope
    guards.py            same-origin JSON rule for unsafe methods
    headers.py           Cache-Control defaults, HEAD for every GET
    schemas.py           ApiModel, ErrorBody, FieldError
    serve.py             uvicorn and watchfiles behind web --server fastapi
    sources/
      router.py          GET /api/reader/sources
      schemas.py         SourcesResponse, one variant per source kind
      deps.py            picks the adapters; tests override them here
      service.py         SourceService, ports SourceCatalog and CollectionStats
      model.py           SourceSummary, XActivity, summarize(), source_kind()
      clickhouse.py      ClickHouseCollectionStats
      config_catalog.py  ConfigSourceCatalog, over Settings.sources
```

A feature skips files it has nothing for: no `model.py` without rules, no
adapter without I/O.

## Principles

1. **Features are modules.** Each lives in `dank/api/<feature>/` with the same
   file names, so route groups can be ported in parallel without touching each
   other's files.
2. **Dependencies point inward.** `model.py` and `service.py` import no
   FastAPI, Starlette, Pydantic, SQLAlchemy, ClickHouse, aiohttp or `dank.web`
   code. `run-linters.sh` checks this.
3. **Pydantic at the edge, NamedTuples inside.** Request and response models
   extend `ApiModel` (`extra="forbid"`, frozen). Routes never return dicts or
   NamedTuples, and services never take Pydantic models. A convention test
   checks through OpenAPI that every route returns a strict model.
4. **Controllers are thin.** A route lets Pydantic parse the input, calls one
   service method and shapes the result: no SQL, no `try`/`except`, no
   business rules. Routes are plain functions on an `APIRouter`.
5. **Ports only where I/O happens.** A service declares a `Protocol` for a
   database, the clock, a model, the network or a subprocess. Pure code calls
   pure code directly. Adapters match a port by shape and never inherit.
6. **Repositories for writes, read stores for reads.** ClickHouse reads go
   through read stores that return domain values, never rows. Postgres app
   state, starting with the source catalog, gets repositories.
7. **One aggregate, one transaction.** A repository method opens and commits
   its own transaction. There is no unit-of-work class until a use case must
   change two aggregates together.
8. **Errors are domain facts, translated once.** Services raise `DankError`
   subclasses; `api/errors.py` maps them, and FastAPI's own errors, to one
   envelope. Only edge code raises `HTTPException`.
9. **Resources come from the lifespan.** The ClickHouse client, refresh
   workers and settings are opened in the lifespan and reached through
   `Annotated` dependencies. There are no module-level clients.
10. **Fakes over mocks, real databases over fakes of SQL.** See Testing.

## Errors

Every error has the same body. `error` is a message the reader can show,
`code` is stable for clients to match on, and `fields` lists invalid inputs:

```json
{
  "error": "The request is invalid.",
  "code": "invalid_request",
  "fields": [
    {"path": "body.n", "code": "int_parsing", "message": "Input should be a valid integer, unable to parse string as an integer"}
  ]
}
```

| Raised by | Status | `code` |
| --- | --- | --- |
| Pydantic validation | 400 | `invalid_request`, with `fields` |
| `InvalidInput` | 400 | `invalid_input`, or a subclass's code |
| `NotFound` | 404 | `not_found` |
| `Conflict` | 409 | `conflict`, or a subclass's code |
| Unsafe method from another origin | 403 | `forbidden` |
| Unsafe method without a JSON body | 415 | `unsupported_media_type` |
| Unknown path or method | 404, 405 | `not_found`, `method_not_allowed` |
| Anything else (ClickHouse is down) | 503 | `unavailable`, logged |

Validation errors use 400 rather than FastAPI's 422, as the aiohttp server
does. A route that needs resources but runs without the lifespan, such as a
test missing a dependency override, answers 503.

## Same-origin rule

`guards.same_origin_json` runs before every route and acts only on unsafe
methods. It rejects `Sec-Fetch-Site: cross-site`, and an `Origin` other than
the scheme and `Host` this server sees. The body must be `application/json`.

The local HTTPS proxy checks the browser's `https://` origin itself, then
rewrites `Origin` to `http://` before forwarding and adds
`X-Forwarded-Proto: https`. The guard therefore ignores forwarded headers, and
uvicorn runs with `proxy_headers=False` so that a proxy on the same machine
can't change the scheme. The rule needs no configuration natively, in Docker or
behind the proxy.

## Headers

- `/api/*` responses get `Cache-Control: no-store` unless the route sets its
  own.
- `/static/*` and HTML responses get the aiohttp server's no-cache headers.
- `HEAD` runs the matching `GET` route and drops the body.

## Startup and shutdown

Both servers run the same ClickHouse startup steps, in order, from
`dank/web/startup.py`: `reddit.sql`, then YouTube, link and reference storage.
The refresh manager starts after them. At shutdown uvicorn gives in-flight
requests 10 seconds, then the lifespan waits for refresh workers (about 95
seconds). That stays within the 110-second `stop_grace_period` in
`compose.yaml`.

## Testing

Tests live in `tests/api/`, mirroring the package. `unittest.mock` is banned
there by `tests/api/ruff.toml`.

- **Domain tests** call pure functions with plain values. Nothing is faked.
- **Route tests** call the real app over `httpx.AsyncClient` with
  `ASGITransport`. Only driven adapters are replaced, with one
  `dependency_overrides` line each, by fakes in the feature's `fakes.py` that
  hold plain values and are type-checked against their port.
- **Adapter tests** run each adapter against a real, throwaway ClickHouse
  database. They are marked `clickhouse` and skipped unless
  `--clickhouse-url` names a loopback server:

  ```sh
  docker run -d --name dank-test-clickhouse \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 -p 127.0.0.1:18123:8123 \
    clickhouse/clickhouse-server:25.8
  uv run pytest tests/api --clickhouse-url=http://default:@127.0.0.1:18123
  ```

  Each test gets a new database with `schema.sql` and the startup steps
  applied, and drops it afterwards. The network guard allows that one address.

Rules: no `AsyncMock`, `patch` or `monkeypatch.setattr` on DANK modules; no
assertions on SQL text, call counts or call order. A test that needs a patch
points to a missing port.

While both servers exist, `tests/api/sources/test_parity.py` starts each one
over the same seeded database and compares the JSON. The one intended
difference: when `reddit.com` has posts but no configured communities, FastAPI
still returns empty `communities` and `flairs` lists.

## Porting a route

1. Copy the route's current assertions into a route test under
   `tests/api/<feature>/` and watch it fail.
2. Move pure logic into `model.py` as NamedTuples and functions. Move it rather
   than rewrite it; the aiohttp module imports it back.
3. Put SQL behind a read store in `clickhouse.py` whose methods return domain
   values.
4. Write the use case in `service.py`, gathering independent reads.
5. Declare query, body and response models in `schemas.py`, extending
   `ApiModel`.
6. Write the route, wire adapters in `deps.py`, and mount the router in
   `app.py`.
7. Add the route to the parity test, and delete the mock-based tests the new
   ones replace.
8. Cover any new SQL with adapter tests in the ClickHouse lane.

## Not built on purpose

Controller classes, base repository or service classes, a unit of work, a
dependency-injection library, an event bus, a mapping library,
`pydantic-settings` or environment variables, generic CRUD routes and API
version prefixes. Each waits for a concrete need.
