# API

DANK's web server is a FastAPI app in `src/dank/api/`, which `web` runs with
uvicorn:

```sh
uv run web                           # reloads on changes under src/dank
uv run web --no-reload --port 8081
```

`web` takes `--config`, `--host`, `--port` and `--no-reload`. OpenAPI is served
at `/api/openapi.json`, with interactive docs at `/api/docs`. The aiohttp server
it replaced was removed in [DEN-51](https://linear.app/dense-analysis/issue/DEN-51);
aiohttp is still DANK's HTTP client for collection.

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
  errors.py              DankError, InvalidInput, NotFound, Conflict, Busy,
                         TooManyRequests; FieldProblem for an error's fields
  sources/               Sources people manage, outside api/ because the CLIs
                         use them too
    model.py             Source, normalized(), compile_sources(), encoding
    service.py           SourceService, port SourceRepository
  db/sources.py          PostgresSourceRepository
  api/
    app.py               create_app(): routers, error handlers, guard, headers
    lifespan.py          Resources, opened at startup and closed at shutdown
    deps.py              ResourcesDep, SettingsDep, PostgresSessionDep, single_values
    errors.py            the error envelope
    guards.py            same-origin JSON rule for unsafe methods
    headers.py           Cache-Control defaults, HEAD for every GET
    schemas.py           ApiModel, ErrorBody, FieldError
    serve.py             uvicorn and watchfiles behind web
    sources/
      router.py          GET /api/reader/sources
      schemas.py         SourcesResponse, one variant per source kind
      deps.py            picks the adapters; tests override them here
      service.py         SourceService, ports SourceCatalog and CollectionStats
      model.py           SourceSummary, XActivity, summarize(), source_kind()
      clickhouse.py      ClickHouseCollectionStats
      config_catalog.py  ConfigSourceCatalog, over Settings.sources
    source_settings/
      router.py          GET, POST and PUT /api/reader/source-settings
      schemas.py         SourceBody, SourceOut, SourceListResponse
      deps.py            picks the repository; tests override it here
    posts/
      router.py          GET /api/reader/posts, /post and /x/authors
      schemas.py         Query models; PostOut, PostDetailOut, XAuthorsResponse
      deps.py            picks the adapters; tests override them here
      service.py         PostService, ports PostStore, SearchEmbeddings, Clock
      model.py           ReaderFilters, reader_filters(), cursors, PostRow
      clickhouse.py      ClickHousePostStore, over the reader helpers
      embeddings.py      CachedSearchEmbeddings: cache first, model in a thread
    connections/
      router.py          GET /api/reader/connections and /connection
      schemas.py         Query models; ConnectionsResponse, ConnectionOut
      deps.py            picks the adapter; tests override it here
      service.py         ConnectionService, port ConnectionStore
      model.py           ConnectionScope, ConnectionKey and their rules
      clickhouse.py      ClickHouseConnectionStore, over the reader helpers
    source_icons/
      router.py          GET /api/reader/source-icon, a PNG
      schemas.py         SourceIconQuery
      deps.py            picks the adapters; tests override them here
      service.py         SourceIconService, ports IconCache, CollectedDomains
      model.py           source_icon_url(), which domains may have an icon
      clickhouse.py      ClickHouseCollectedDomains
    files/
      router.py          GET /reader and /reader/..., the redirect to /
      static.py          ReaderFiles, FileRoots, the /assets mount and the reader
      model.py           client_route(), which paths load the reader
    youtube/
      router.py          GET and POST /api/reader/youtube/...
      schemas.py         Query models; YouTubeItemOut, PlaylistOut and the rest
      deps.py            picks the adapters; tests override them here
      service.py         YouTubeService, VideoCaptureService; ports YouTubeStore,
                         VideoCollector; CollectionLocks, CollectionLimits
      model.py           YouTubeFilters, PlaylistPage, cursors, transcript_view()
      clickhouse.py      ClickHouseYouTubeStore, over the reader helpers
      collector.py       ScrapeVideoCollector, CollectionGate, the real waits
    refresh/
      router.py          GET and POST /api/reader/refresh
      schemas.py         RefreshRequest, one scope variant per kind; RefreshJobOut
      deps.py            picks the adapters; tests override them here
      service.py         RefreshService, resolve_scope, ports RefreshTargets, RefreshQueue
      model.py           RefreshPlan, RefreshJob, scope and history rules
      clickhouse.py      ClickHouseRefreshTargets
      manager.py         RefreshManager, running_refresh, port CollectionWorker
      worker.py          ProcessWorker, one spawned process per job
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
   checks through OpenAPI that every route returns a strict model. Wide
   projections are the one exception inside; see Projections. Pages and files
   have no JSON to model; see Pages and files.
4. **Controllers are thin.** A route lets Pydantic parse the input, calls one
   service method and shapes the result: no SQL, no `try`/`except`, no
   business rules. Routes are plain functions on an `APIRouter`.
5. **Ports only where I/O happens.** A service declares a `Protocol` for a
   database, the clock, a model, the network or a subprocess. Pure code calls
   pure code directly. Adapters match a port by shape and never inherit.
6. **Repositories for writes, read stores for reads.** ClickHouse reads go
   through read stores that return domain values or projections, never rows.
   Postgres app state, starting with sources, gets repositories.
7. **One aggregate, one transaction.** A repository method opens and commits
   its own transaction. There is no unit-of-work class until a use case must
   change two aggregates together.
8. **Errors are domain facts, translated once.** Services raise `DankError`
   subclasses; `api/errors.py` maps them, and FastAPI's own errors, to one
   envelope. Only edge code raises `HTTPException`.
9. **Resources come from the lifespan.** The ClickHouse client, Postgres
   sessions, refresh workers, collection locks and limiters, and settings are
   opened in the lifespan and reached through `Annotated` dependencies. There
   are no module-level clients.
10. **Fakes over mocks, real databases over fakes of SQL.** See Testing.

## Errors

Every error under `/api/` has the same body. `error` is a message the reader can
show, `code` is stable for clients to match on, and `fields` lists invalid
inputs:

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
| `InvalidInput` | 400 | `invalid_input`, or a subclass's code, with `fields` when it names them |
| `NotFound` | 404 | `not_found` |
| `Conflict` | 409 | `conflict`, or a subclass's code |
| `Busy` | 503 | `busy` |
| `TooManyRequests` | 429 | `too_many_requests` |
| Unsafe method from another origin | 403 | `forbidden` |
| Unsafe method without a JSON body | 415 | `unsupported_media_type` |
| Unknown path or method | 404, 405 | `not_found`, `method_not_allowed` |
| Anything else (ClickHouse is down) | 503 | `unavailable`, logged |

Validation errors use 400 rather than FastAPI's 422, as the reader expects.
OpenAPI declares the envelope as every route's `default` response, in place of
FastAPI's 422, so the reader's generated types know it.
A route that needs resources but runs without the lifespan, such as a
test missing a dependency override, answers 503.

Everywhere else, the same errors answer with the same status and `error` as
plain text. No page or file has ever answered in JSON.

## Query models

A route takes its query as `Annotated[Model, Query()]`, which gives OpenAPI one
parameter per field. The model extends `ApiModel`.

- **Repeatable keys** are `tuple[str, ...]` fields.
- **Unknown keys** fail through `extra="forbid"`. A route that has always
  ignored them says so with `extra="ignore"`.
- **Repeated scalar keys** need `dependencies=[Depends(single_values(Model))]`
  on the route. Starlette keeps only a key's last value, so Pydantic never sees
  the repeat.
- **Shape is Pydantic's.** Types and closed sets of values fail as
  `invalid_request` and name their field in `fields`.
- **Rules are the domain's.** A rule across fields, or one whose wording the
  reader shows, lives in `model.py` and raises `InvalidInput`, so `error` keeps
  that wording. The model hands its values to the rule from a method such as
  `query.filters()`, called by the route.
- **Keep a field as text when Pydantic is looser than the rule.** Pydantic takes
  `5.0` as an integer and a timestamp as a date. Run a `BeforeValidator` on the
  rule, or validate the text in the domain. `InvalidInput` is a `ValueError`, so
  Pydantic reports a rule that a validator calls as a field error.
- **Absent can differ from empty.** Use `| None = None` when sending a key empty
  must fail while leaving it out is fine.

## Path parameters

A route declares its shape as `Annotated[str, Path(pattern=...)]`, so a bad value
fails as `invalid_request` and OpenAPI shows the rule. Keep Starlette's default
converter, which can't match a slash; never use `:path`. uvicorn decodes the path
before routing, so `%2F` is a slash too, and finds no route.

## Collecting on demand

`POST /api/reader/youtube/metadata/{video_id}` and `/transcript/{video_id}` ask
YouTube about one video that DANK already knows, then save the answer. Asking
about an unknown ID fails before anything waits or connects. So that readers
can't pile up behind a slow provider:

- One collection of each kind runs at a time. Its `asyncio.Lock` and
  `RequestLimiter` make up a `CollectionGate`, and the lifespan owns one for each
  kind. A request that waited reuses what the first one saved.
- The service bounds each wait with `CollectionLimits`, and running out raises
  `Busy`. Metadata gets 15 seconds for the lock and the collection together.
  Transcripts get 1 second for the lock, then the transcript timeout plus 5
  seconds for the lookup.
- Tests override `collection_limits` with short waits and hold a collection open
  with the fake collector, so nothing has to sleep for the real waits.

Each body is declared as `CollectBody`, an empty object. The reader sends `{}`.

## Projections

Posts, connections and YouTube items are the exception to NamedTuples inside. The
helpers in `dank/web/reader_*.py` build one deeply nested, JSON-shaped dict per
post, with keys that are absent rather than null. `model.py` names the dict
(`PostPayload`, `ItemPayload`) and `schemas.py` gives it its contract. The
aiohttp server that also needed the dicts is gone, so they could now become
NamedTuples.

YouTube's keys are always present, even when null, so its routes need none of the
following. To keep absent fields absent:

- Declare a field that may be absent with a `None` default, and one that is
  always present, even as null, without a default.
- Build the response with `model_validate` on the dict, so a missing key stays
  unset. Don't pass `None` for it.
- Set `response_model_exclude_unset=True` on the route.

The models forbid unknown keys, so a helper that gains a field answers 503 until
its schema declares it. Check a schema change against real data, because
fixtures miss what scrapes actually saved.

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

FastAPI parses a JSON body before it runs dependencies, so malformed JSON
answers 400 even from another origin. Nothing has run by then.

## Headers

- `/api/*` responses get `Cache-Control: no-store` unless the route sets its
  own. Starlette answers unexpected errors outside every middleware, so that
  handler sets the header itself.
- HTML responses get no-cache headers.
- `HEAD` runs the matching `GET` route and drops the body, except on the file
  mounts, which answer it themselves. Run as a `GET`, a `HEAD` on a video would
  read the whole file only to throw it away.

## Pages and files

These routes answer with HTML, files or images, so they have no response model.
The `/reader` redirect stays out of OpenAPI with `include_in_schema=False`. `/api/reader/source-icon` stays in and declares
`image/png`.

`/assets/` is a Starlette `StaticFiles` mount, and the reader is a `StaticFiles`
app set as the router's fallback, so it answers any path no route or mount
matched, however late the route was added. Both
meet what audio and video need, as `tests/api/files` checks over HTTP: single and
open-ended ranges, `If-Range`, `If-None-Match`, `If-Modified-Since`, 416 past the
end, and `HEAD` without reading the file. No responder of our own was needed.
`ReaderFiles` adds only what is the reader's:

- A missing path falls back to `index.html` only if it has no extension and stays
  inside the build, so a missing bundle answers 404 rather than HTML.
- `index.html` is `no-store` even on a 304, which has no content type for the HTML
  header rule to catch.
- A checkout with no reader build answers 404 rather than failing.
- Unmatched `/api/...` and `/assets` paths keep their 404, whatever the method,
  rather than loading the reader or answering 405.
- Bundles live under `/bundle/`, because `/assets/` holds downloaded media.

`/reader` and `/reader/...` redirect to the same path and query at `/` with a
relative `Location`, because behind the HTTPS proxy this server sees plain HTTP.
For the same reason `redirect_slashes` is off: `/assets` is a 404, not a redirect to an `http://` address.

The Jinja viewer that `/` and `/post` used to serve was retired in
[DEN-75](https://linear.app/dense-analysis/issue/DEN-75), and the reader moved
to `/` in [DEN-77](https://linear.app/dense-analysis/issue/DEN-77).

## Startup and shutdown

Both servers run the same ClickHouse startup steps, in order, from
`dank/web/startup.py`: `reddit.sql`, `categories.sql`, then YouTube, link and
reference storage. The refresh manager starts after them. At shutdown uvicorn gives in-flight
requests 10 seconds, then the lifespan waits for refresh workers (about 95
seconds). That stays within the 110-second `stop_grace_period` in
`compose.yaml`.

## Refresh jobs

The lifespan owns one `RefreshManager` through `running_refresh`: it restores
`data_dir/reader-refresh.json`, runs the scheduler task and calls `close()` at
shutdown. `RefreshService` is the use case the routes call, built per request
like any other; it resolves a scope through `RefreshTargets`, then submits it to
the manager through `RefreshQueue`.

- **Running a job is a port.** `ProcessWorker` spawns a process per job, whose
  entry point is `dank/web/refresh_worker.py`, and route tests swap it for a
  fake. The history file isn't a port, since a temporary file is as easy as a
  fake and more real.
- **A scope is JSON-shaped.** It is saved and answered with keys absent rather
  than null, as in Projections. A job is a NamedTuple around it.
- **Rejections keep their wording.** `resolve_scope` turns a plain `ValueError`
  from `urlsplit` or `configured_feeds` into `InvalidInput`, so it answers 400
  with their words rather than 503. Their messages can say what to configure.
- **Restoring is strict.** The API's models forbid what they don't know, so
  `restored_jobs` leaves out any saved job they couldn't describe, rather than
  letting one answer 503 for the whole list. Unknown fields are dropped.
- **A worker stops in one of two ways.** Shutdown signals it and waits up to 90
  seconds, then kills its process group. If the server dies or reloads first,
  which `watchfiles` forces after 5 seconds, `watch_owner` sees the pipe close
  and stops the collection. The next start marks the job interrupted.

## Reader types

`dank-openapi` writes the app's schema to `reader/openapi.json` without starting
a server, and the reader generates `reader/src/lib/api.gen.ts` from it. Run both
with `make openapi` after changing a route or a model, and commit the two files.
`tests/api/test_openapi_file.py` fails while the schema is stale. Two models with
one name get mangled schema names, so give each response model a unique name.
A `Literal` is worth declaring wherever the reader relies on a closed set: it
becomes a union type there. See [the reader's API types](../reader/README.md#api-types).

## Testing

Tests live in `tests/api/`, mirroring the package. `unittest.mock` is banned
there by `tests/api/ruff.toml`.

- **Domain tests** call pure functions with plain values. Nothing is faked.
- **Route tests** call the real app over `httpx.AsyncClient` with
  `ASGITransport`. Only driven adapters are replaced, with one
  `dependency_overrides` line each, by fakes in the feature's `fakes.py` that
  hold plain values and are type-checked against their port.
- **Repository tests** run Postgres repositories against a real database, and
  are marked `postgres`; see [Postgres migrations](development.md#postgres-migrations).
  `tests/db/test_sources.py` races real transactions, which a fake can't.
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
- **Projection tests** do the same over HTTP: the real router and service, with
  the real store swapped in for the fake. Each test states its rows and checks
  the JSON, as in `tests/api/posts/test_payloads.py`.
- **Process tests** use real processes, signals and pipes, as in
  `tests/api/refresh/test_worker.py`. `tests/api/refresh/test_lifecycle.py` runs
  whole jobs through real workers, collecting from a feed server on the same
  machine.

Rules: no `AsyncMock`, `patch` or `monkeypatch.setattr` on DANK modules; no
assertions on SQL text, call counts or call order. A test that needs a patch
points to a missing port.

Semantic search reads `web_embedding_cache`, so no adapter test loads the model;
date those rows from the clock, because the table's TTL drops old ones.

## Adding a route

1. Write a route test under `tests/api/<feature>/` and watch it fail.
2. Put pure logic in `model.py` as NamedTuples and functions.
3. Put SQL behind a read store in `clickhouse.py` whose methods return domain
   values.
4. Write the use case in `service.py`, gathering independent reads.
5. Declare query, body and response models in `schemas.py`, extending
   `ApiModel`. See Query models and Projections.
6. Write the route, wire adapters in `deps.py`, and mount the router in
   `app.py`. Add `single_values` if the route takes a query model.
7. Cover any new SQL with adapter tests in the ClickHouse lane.

## Not built on purpose

Controller classes, base repository or service classes, a unit of work, a
dependency-injection library, an event bus, a mapping library,
`pydantic-settings` or environment variables, generic CRUD routes and API
version prefixes. Each waits for a concrete need.

### Saved post categories

`GET /api/reader/categories` returns configured category names as
`{ "categories": ["science"] }`. Choices come from `categories.definitions`
in the loaded `config.toml`; this endpoint does not access ClickHouse.
Reader feed and post detail payloads include `categories`, an array of
`{ "name": "science", "confidence": 0.9, "similarity": 0.95 }` objects.
An empty array means no saved assignment. The scores describe the matching
training example's confidence and cosine similarity, not a new probability.

Repeat `category` on `/api/reader/posts` (and X author listings) to match any
selected exact, case-sensitive name. Category filtering combines with other
filters using AND and is bound into pagination cursors. Category choices do
not trigger training or processing. Both servers provide these routes.
