# Sources in Postgres

DANK is moving its sources from `config.toml` into Postgres, so they can be
edited without a restart. This page covers the `sources` table, a one-time
import, the `sources_in_database` switch that puts the table in use, and an
API. Editing sources in the reader comes with
[DEN-21](https://linear.app/dense-analysis/issue/DEN-21).

`sources` in `config.toml` is deprecated. Import it once, turn the switch on,
and keep your sources in Postgres from then on.

## Import config.toml once

```sh
make sources-import              # Docker
make sources-import MODE=native  # native
```

The command saves every source in `config.toml` in Postgres, in the same order,
and prints how many it imported:

```text
Imported 14 sources into Postgres database dank.
DANK still uses config.toml's sources. To use these instead, set this near the top of config.toml, before any [section], replacing any sources_in_database line already there, and restart DANK:

sources_in_database = true
```

With the switch already on, the last line says `DANK uses these sources now.`

- **It runs once per database.** Once any source exists, it says
  `Sources were already imported, so nothing changed.` and exits 1. Two runs
  started together import once.
- **It's all or nothing.** If one source can't be imported, nothing is, and the
  command lists each problem with the source's domain and its place, counting
  from 0, such as `sources.3.domain`. The count leaves out entries `config.toml`
  itself ignores, such as one without a domain.
- **It's stricter than `config.toml`.** A domain must be a host name, such as
  `example.com` rather than `https://example.com`, and its feed URLs must be
  ones that platform collects.
- **Exact duplicates are saved once,** and the command says how many it skipped.
  An account listed twice in one source is kept once, whitespace around it is
  removed, and a blank one is left out.
- **An empty list imports nothing,** so you can import later.
- It needs the schema at head and never migrates. Start the web app, or run
  `make db-upgrade`, first.
- It never edits `config.toml`.

To start again while the data doesn't matter, empty the table and import again.
With Docker:

```sh
docker compose exec postgres psql --username=dank --dbname=dank -c 'TRUNCATE sources'
make sources-import
```

Each worktree has its own database, and imports from its own `config.toml`.

## Use the sources in Postgres

`sources_in_database` is a top-level key in `config.toml`, so it goes before
any `[section]`. It decides where every source comes from:

| `sources_in_database` | Sources come from |
| --- | --- |
| `false`, the default | `sources` in `config.toml`, read when DANK starts |
| `true` | The `sources` table only, even when it's empty. DANK no longer uses `sources` in `config.toml`, though it still checks it at startup, so remove it or keep it valid |

Changing it needs a restart: `make web ARGS='--force-recreate dank'` with
Docker, which copies `config.toml` in when the container starts. Switching
off goes back to `config.toml` and keeps the table, so you can switch on again
later.

With the switch on:

- **Edits apply without a restart.** Each reader request, refresh and command
  reads the table as it starts. That covers the sidebar and its counts, tags,
  configured YouTube channels, which source icons the reader serves, `scrape`,
  `process` and `train`.
- **Submitted work keeps its sources.** A queued or running refresh collects
  the sources it was submitted with. A refresh asked for after an edit uses the
  new ones; asking again for the same refresh joins the active job only while
  the sources it selects are unchanged.
- **Disabled sources stop new collection only.** Their posts stay in the
  sidebar, readable, searchable and processable, and so do a disabled account's
  or feed's.
- **Other tabs catch up within five seconds.** A reader tab in the foreground
  checks the source list every five seconds, and again when you come back to
  it, and reloads its sidebar when the list changed.
- **An empty table means no sources.** The reader shows its empty state, and
  `scrape`, `process` and `train` stop with
  `No sources in the database. Run make sources-import, or add one in the app.`
  and exit 1.
- **Postgres has to be up.** An outage never falls back to `config.toml`:
  reader requests that need sources answer `503`, and commands stop with
  `sources_in_database is on, but the sources couldn't be read from Postgres:`
  and the reason, and exit 1. Commands also need the schema at head, and never
  migrate. Scrape modes that name their own posts, such as
  `--x-post`, read no sources and run without Postgres.
- **It needs Postgres turned on.** With `[postgres] enabled = false`, DANK
  refuses the config:
  `sources_in_database needs Postgres, which is turned off ([postgres] enabled = false).`

`GET /api/reader/sources`, the sidebar's request, has an
`in_database` flag that mirrors the switch, so the reader knows whether to watch
for edits.

## What a source is

A source has the fields of a `config.toml` source, plus an ID the server gives it
and an enabled flag on the source and on each account and feed.

| Field | Holds |
| --- | --- |
| `id` | A UUID, given by the server and never changed |
| `enabled` | Whether DANK collects it; disabled sources stay readable |
| `domain` | The canonical domain: `www.reddit.com`, `hckrnews.com` and similar spellings are folded |
| `accounts` | X accounts or YouTube channels, each `{value, enabled}` |
| `feeds` | Feed URLs, Reddit communities or YouTube listings, each `{value, enabled}`, stored normalized |
| `tags` | Lowercase, without repeats |
| `adapters` | `null` for the domain's default, or a list of `{kind, feeds}`; only RSS adapters have `feeds`, and `null` there means the source's own |
| `reddit` | `{sort, time_filter}` for Reddit, otherwise `null` |
| `hacker_news` | `{filter}` for Hacker News, otherwise `null` |

The rules are `config.toml`'s, from the same functions: Reddit takes
communities, not accounts; X takes accounts, not feeds; RSS adapters only work
for publishers and Hacker News; and Hacker News feeds need an RSS adapter. Two
sources can share a domain when their settings differ, but two sources with
identical settings can't exist, whichever is enabled.

**Turning targets off never collects more.** DANK reads an empty list as
"everything": an X source with no accounts means For You, and a publisher with no
feeds means feed discovery. So when every account or feed in a list is off, that
way of collecting is removed rather than run without them, and a source with
nothing left isn't collected.

`dank.sources.model.compile_sources` turns sources into the `SourceConfig`
values the collectors already use. Sources imported from `config.toml` compile
back to exactly what the TOML gave, which `tests/sources/test_model.py` checks.

## The table

`sources` holds one row per source: `id`, `position` (the order sources were
added in, which the import takes from `config.toml`), `enabled`, and `config`,
the rest as JSON. A unique index on a hash of `config` refuses duplicates.

A save changes one row in one short transaction. There's no version history,
so an edit can't be undone in the app; back up with `make postgres-dump`. When
two tabs edit different sources, both edits stick. When they edit the same one,
the last save wins.

## API

Under `/api/reader/source-settings`. See
[the API conventions](../api.md).

| Method | Path | Does |
| --- | --- | --- |
| `GET` | `/api/reader/source-settings` | `{sources: [...], in_database}`: every source in order, disabled ones too, and whether DANK uses them, which is false while the switch is off |
| `GET` | `/api/reader/source-settings/{id}` | One source |
| `POST` | `/api/reader/source-settings` | Add a source, sent without an ID. Answers `201` with the saved source |
| `PUT` | `/api/reader/source-settings/{id}` | Replace a source, including whether it and each target are enabled |

Every write sends the whole source; a missing field is an error, not a way to
clear it. The answer has the values normalized, so `r/LocalLLaMA` comes back as
`https://www.reddit.com/r/LocalLLaMA/`. Sources can't be deleted yet; disable
them instead.

| Status | `code` | When |
| --- | --- | --- |
| 400 | `invalid_request` | The body has the wrong shape, an unknown key or a list over its limit |
| 400 | `invalid_source` | A rule is broken. `fields` lists each problem at its path in the source, such as `feeds.0.value` |
| 404 | `not_found` | No source has this ID |
| 409 | `duplicate_source` | Another source has identical settings, enabled or not |
| 503 | `unavailable` | Postgres is unreachable, or a stored row can't be read. Logged |

Retrying is always safe. Repeating an edit changes nothing further, and a
repeated add answers `duplicate_source`, which means the first one worked.

## Change a source's shape

Everything except the ID and enabled flag lives in the `config` column, so
changing what a source holds needs no change to the table.

- **A new field with a default:** change the model, and let `decode_source`
  read rows saved without it, which it otherwise refuses. The next save writes
  the field. Or add it to every row with a data migration, as below.
- **A breaking change while the data doesn't matter:** change the model, empty
  the table, and import again.
- **A breaking change once other people have data:** change the model and write
  a data migration by hand, for example:

  ```sh
  make db-atlas ARGS='migrate new rename_feeds --env dank'
  ```

  ```sql
  UPDATE sources
  SET config = (config - 'feeds') || jsonb_build_object('feed_urls', config -> 'feeds');
  ```

  ```sh
  make db-atlas ARGS='migrate hash --env dank'
  ```

Rows are read strictly: a missing or unknown key, or a value of the wrong type,
answers `503` rather than being skipped. The web app migrates before it starts and
CLIs refuse a schema that isn't at head, so code never meets a row in an old
shape. Update `sources_from_settings` with the model; the parity tests fail if
it no longer matches.

## Limits

Project heuristics far above a real source, in `dank/sources/model.py`: 1,000
targets in a list, 32 tags, 16 adapters and 2,048 characters in a value,
measured after normalizing. The API also
needs a request size limit
([DEN-58](https://linear.app/dense-analysis/issue/DEN-58)).
