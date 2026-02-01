# DANK - Agent instructions

Make sure to read and follow the description of the project and the spirit of it
from README.md. You are tasked with building a locally hosted tool that scrapes
information off of the public Internet.

## Libraries

You should stick to using the following libraries.

* Use `zendriver` for web browser scraping.
* Use `yt-dlp` for scraping videos and music content off sites like YouTube.
* Use `aiohttp` for basic HTTP web scraping.

## Configuration

Configuration lives in `config.toml` and should never be committed.

## Architecture

The DANK system should be built as an asynchronous framework for scraping
information from sites with parallel processing, limiting requests to specific
sites to avoid hitting an individual host too often. Where possible when
using web scraping `zendriver` should connect to hosts and pull information from
the web requests the browser is sending instead of scraping HTML content on
the pages.

* Code in the `scrape` module should only be concerned with scraping data, and
  should know nothing about the database or how processing is done. We should
  be capturing data as raw as possible and storing JSON payloads of data to
  process.
* Code in the `process` module should only be concerned with loading data from
  the database, processing it, and storing it back again in a processed form.
* Never import `scrape` code in the `process` module or vice versa. Never
  import `clickhouse` code into the `scrape` module.

## Basic Principles

* Use `asyncio` as much as possible for great concurrency.
* Never ever use a Python `dataclass` if at all possible.
* Use `NamedTuple` for abstract data types as much as possible to pass immutable
  data around.
* Store original payloads of data scraped from sites as JSON in the database.
* Never use `os.environ` for configuration in code, because all configuration
  should belong in the TOML configuration.
* Implement tests

## Tools

* Run `./run-linters.sh` to run all linters to check for and autofix errors.
* You can run tests with `uv run pytest`.

## Code Style

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

You have access to a local ClickHouse database during development default
ClickHouse credentials on Port 9000, and you can use that for your operations.
