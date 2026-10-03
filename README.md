# DANK - Dense Analysis Network Knowledge

DANK is a locally hosted Dense Analysis tool for collecting and exploring
public Internet content. It collects RSS/Atom articles and X posts, preserves
raw source payloads, downloads media, and prepares posts for browsing and
semantic search using local text embeddings.

The broader goal is to automate contextual understanding of trends, public
perception and evolving narratives.

## Features

- [RSS and Atom feeds](docs/features/rss.md) — discover feeds and collect articles.
- [X accounts](docs/features/x.md) — capture posts from configured accounts.
- [Media downloads](docs/features/media.md) — download and store discovered assets.
- [Processing and embeddings](docs/features/processing.md) — prepare searchable posts.
- [Browsing and search](docs/features/search.md) — explore posts in the web viewer.
- [Database queries](docs/features/database.md) — inspect collected and processed data.

## Requirements

- Python 3.13
- uv
- ClickHouse (local server)

## ClickHouse setup

1. Install ClickHouse: https://clickhouse.com/docs/en/install
2. Start the ClickHouse server (systemd or `clickhouse server`).
3. Create the schema:

```
~/clickhouse/clickhouse client --multiquery < schema.sql
```

The schema uses the `dank` database by default. Adjust `config.toml` if you
need a different database name.

## Configuration

Configuration lives in `config.toml` and should not be committed. Example:

```toml
sources = [
  { domain = "x.com", accounts = ["example"] },
  "blog.codinghorror.com",
]

[clickhouse]
host = "localhost"
port = 8123
database = "dank"
username = "default"
password = ""
secure = false
use_http = true

[x]
username = "your-x-username"
password = "your-x-password"
max_posts = 200
max_scrolls = 20
scroll_pause_seconds = 1.5

[storage]
data_dir = "data"
max_asset_bytes = 10485760

[rss]
# Keep feed entries when fetching their linked article fails.
keep_feed_on_fetch_failure = false

[browser]
# Optional: full path or command name for a Chromium-based browser.
executable_path = "thorium-browser"
# Optional: extra time to wait for the browser to start.
connection_timeout = 1.0
# Optional: connection retry count for slow browser startups.
connection_max_tries = 30

[email]
# Optional: IMAP settings for OTP codes.
host = "imap.example.com"
username = "you@example.com"
password = "your-imap-password"
port = 993

[logging]
# Optional: file path for scrape/process logs.
file = "dank.log"
# Optional: logging level (DEBUG, INFO, WARNING, ERROR).
level = "INFO"
```

`sources` controls which domains to scrape and process. Each entry can provide
accounts for account-based sources like `x.com`.

If any particular domain lacks a specific configuration, the root of the
domain will be scraped to discover RSS feeds to read from.

`rss.keep_feed_on_fetch_failure` defaults to `false`. Set it to `true` to
retain RSS/Atom entries when their linked article cannot be fetched or returns
an empty body. Processing uses the available feed content, and the raw payload
records `page_fetch_status = "failed"`. This does not schedule retries.

`browser.executable_path` sets the browser binary to launch. If unset, DANK
will try common Chromium locations.

`storage.max_asset_bytes` caps asset downloads (bytes). Larger assets are
skipped but still recorded.

When X prompts for a one-time code, DANK will poll the IMAP inbox for messages
from `x.com` that arrived after the login attempt and extract the confirmation
code.

If the browser takes longer to start, increase
`browser.connection_timeout` or `browser.connection_max_tries`.

`logging.file` controls where scrape/process logs are written. Relative paths
are resolved from the current working directory.

## Usage

Dank offers the following commands.

* `uv run scrape` -- Scrape the web for data
    * Pass `--domains` to scrape only matching domains from `sources`,
      for example `--domains '^x\.com$'`.
* `uv run process` -- Process previously scraped data
    * The `--age` argument can be given a duration to process, for example
      `6hours` or `48h`.
* `uv run clickhouse-query` -- Run queries on the database
    * You can only run `SELECT`, `SHOW`, or `EXPLAIN` queries through this tool
    * Query results are well formatted and easy to read
    * Query results are truncated unless you pass `--full`
* `uv run embed-text "your text"` -- Print an embedding vector
    * Output is a JSON `list[float]` for easy copy/paste into other tools.
* `uv run download-embedding-model` -- Download and cache embeddings model
    * Pass `--model` to choose another Hugging Face model id.
* `uv run web` -- Start a simple web server to view content.
    * Pass `--no-reload` to disable hot code reloading.
    * Supports search filters for domain/account and a days-back slider.

## Testing

* `uv run pytest` -- Run default test suite.
* `uv run pytest -m embeddings -s` -- Run real-model embedding checks.
    * These tests are skipped by default and require the model cache.
    * Includes per-case similarity and margin output for each model.
