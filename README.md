# DANK - Dense Analysis Network Knowledge

DANK is a Dense Analysis project focused on collecting and analyzing live data
from the public Internet. It uses API access, web scraping, RSS feeds, and
semantic indexing tools to ingest external content in real time. It applies
sentiment analysis, semantic clustering, and AI models to build structured
insights about the world, including trends, public perception, and evolving
narratives. The goal is to automate contextual understanding and surface
relevant knowledge as it emerges.

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
assets_dir = "data/assets"
max_asset_bytes = 10485760

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
```

`sources` controls which domains to scrape and process. Each entry can provide
accounts for account-based sources like `x.com`.

`browser.executable_path` sets the browser binary to launch. If unset, DANK
will try common Chromium locations.

`storage.max_asset_bytes` caps asset downloads (bytes). Larger assets are
skipped but still recorded.

When X prompts for a one-time code, DANK will poll the IMAP inbox for messages
from `x.com` that arrived after the login attempt and extract the confirmation
code.

If the browser takes longer to start, increase
`browser.connection_timeout` or `browser.connection_max_tries`.

## Usage

Scrape configured sources:

```
uv run python -m dank.scrape --config config.toml --headless
```

Process raw posts into normalized posts:

```
uv run python -m dank.process --config config.toml --limit 500
```
