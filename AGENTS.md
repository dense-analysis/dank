# DANK - Agent instructions

Make sure to read and follow the description of the project and the spirit of it
from README.md. You are tasked with building a locally hosted tool that scrapes
information off of the public Internet.

## Libraries

You should stick to using the following libraries for scraping.

* Use `zendriver` for web browser scraping.
* Use `yt-dlp` for scraping videos and music content off sites like YouTube.
* Use `aiohttp` for basic HTTP web scraping.

## Configuration

Configuration lives in `config.toml` and should never be committed. You must
never read the values from `config.toml`, only pass them on to internal
functions in code for running DANK. Treat the Git-ignored `postgres-password`
file the same way, and never put its contents in logs or error messages.

## Architecture

The DANK system should be built as an asynchronous framework for scraping
information from sites with parallel processing, limiting requests to specific
sites to avoid hitting an individual host too often. Where possible when
using web scraping `zendriver` should connect to hosts and pull information from
the web requests the browser is sending instead of scraping HTML content on
the pages.

* Code in the `scrape` module should only be concerned with scraping data.
  Only in `dank/scrape/runner.py` should write to the ClickHouse database
  or to the filesystem for assets. Submodules should only be concerned with
  discovering posts and assets that need persisting.
* Code in the `process` module should only be concerned with loading data from
  the database, processing it, and storing it back again in a processed form.
* Code in the `training` module owns loading training posts, calling Decisions,
  and persisting category training examples. Only `dank/training/runner.py`
  may access ClickHouse within this module.
* For categories, the `process` module only loads and assigns saved data. It
  must never import from `training` or make training API requests.
* Never import `scrape`, `process`, or `training` code into one another.
  Shared data types and pure calculations belong outside these modules and
  must not import any of them.
* Only `dank/scrape/runner.py`, `dank/process/runner.py`,
  `dank/training/runner.py`, and the web app may access ClickHouse.
* Only `dank/db/atlas.py` may run Atlas, so that it stays replaceable. Change the
  Postgres schema through migrations; see `docs/development.md`.

IMPORTANT: Do not bypass the import boundaries between `scrape`, `process`,
and `training` by creating another module which imports their code. Shared
modules must own independent data types or pure calculations, not redirect
imports or calls into a forbidden module.

## Basic Principles

* You are writing Python 3.12+ code. Never write simple helper functions
  if there are very simple first party library functions like
  `itertools.batched`.
* Code lives in either `src` or `test` directories.
* Use `asyncio` as much as possible for great concurrency.
* Never ever use a Python `dataclass` if at all possible.
* Use `NamedTuple` for abstract data types as much as possible to pass immutable
  data around.
* Store original payloads of data scraped from sites as JSON in the database.
* Never use `os.environ` for configuration in code, because all configuration
  should belong in the TOML configuration.
* Use brief comments for non-obvious reasoning, constraints, and tradeoffs.
  Link to an authoritative source beside a borrowed formula or algorithm.
  Name tunable thresholds and distinguish project heuristics from constants
  defined by a standard; explain what those choices are intended to achieve.
* Separate calculations, I/O, and presentation where their responsibilities
  differ. Extract functions, components, or modules to make ownership clearer;
  keep abstractions proportional to the problem being solved.
* Implement tests, especially unit tests for data processing.
* Track outstanding work and future ideas in the
  [DANK Linear project](https://linear.app/dense-analysis/project/dank-de00db7ebbb2).
  Keep status, priority and action checklists there; repository docs describe
  shipped behavior, operational workarounds and verification evidence.
* Keep Linear statuses current so other people and agents can see what has
  started and what is finished:
  * Before picking up an issue, check that it isn't already In Progress.
  * Move it, and its parent, to In Progress as soon as work starts.
  * Move it to Done once the work is merged into `main`. Comment with the
    commits, how it was verified, any differences from the issue and whether
    it is pushed.
  * Move issues it unblocks from Backlog to To Do, and record follow-up work
    as issues rather than only in chat.
* When changing collection/processing failure handling, quality indicators, or
  source-specific workarounds, update `docs/known-issues.md` with the current
  behavior, remaining limits, and relevant evidence or tests.
* Never use `importlib` in test code for importing private functions, and
  instead import with `# pyright: ignore[reportPrivateUsage]`

## Tools

* Make is DANK's one interface for commands. Whenever you add or change a
  command people run, including a `[project.scripts]` entry, add or update its
  Make target in the same change, list it in `make help`, and add it to the
  command table in `README.md`. `tests/test_makefile.py` checks this.
* Run `make lint` to run all linters to check for and autofix errors.
* You can run tests with `make test`. Pass paths or options through `ARGS`, as
  in `make test ARGS='tests/db -k upgrade'`.
* You can scrape data with `uv run scrape`.
    * Pass `--domains` to scrape only matching `sources` domains.
* You can process scraped data with `uv run process`.
    * You might want to pass `--age 10m` to process content scraped in the
      past 10 minutes, other other ages accepted by `parse_age_window`.
* `make scrape` and `make process` archive timestamped output in
  `logs/scrape/` and `logs/process/`. Use these for recent-run comparisons;
  each log includes command, checkout revision, duration and exit status.
  Search these Git-ignored archives with `rg --no-ignore`.
  `dank.log` remains the combined application log.
* Query with `uv run clickhouse-query -q "<QUERY>"`
    * You can run only `SELECT`, `SHOW`, or `EXPLAIN` queries.
    * Output is truncated by default. To see more information pass the `--full`
      argument.

## Code Style

These readability conventions apply to both Python and TypeScript.

Keep blank lines before new blocks of code, and keep blank lines after new
blocks of code, except when they are at the start or end of a level of
indentation, such as `if` blocks and associated `elif` and `else` blocks,
and `try` and associated `catch` blocks, including all loops, etc. Make sure
we never write blank lines before `elif`, `else`, `catch` etc as they should
be associated with the blocks. Put a blank line before a `return` unless it
starts a level of indentation.

Use PEP 636 pattern matching where appropriate to simplify code, especially
where you have a complex series of nested if statements.

## Database

You have access to the user's local ClickHouse database during development.
Use DANK's configured connection through internal functions or `make query
MODE=native ARGS='-q "SELECT ..."'`; do not inspect private configuration.
The HTTP interface is normally on port 8123 (native protocol on port 9000).
In a sandbox, localhost may be forwarded to the host database.

You may run the app and use Playwright in a browser with the user's saved data
to verify reader changes. Start the backend with `make web MODE=native`
(optionally pass `ARGS='--port 8091'`) and follow `reader/README.md` for the
frontend development server. Check desktop and mobile layouts, actual API
responses, filtering and navigation. Prefer read-only browsing and queries;
do not trigger collection, processing or training just to populate a UI check.
Use isolated fixture data for automated tests that change data. If live access
fails, report the connection failure and which checks used fixtures instead.
Browser testing can use an installed Chromium executable when Chrome is
unavailable; keep environment-specific browser paths out of committed config.

App state goes in Postgres and collected content stays in ClickHouse. See
`docs/features/database.md`. Each worktree needs its own Postgres database on a
shared server; see `docs/development.md`.
