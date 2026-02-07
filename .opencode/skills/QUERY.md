DANK uses a ClickHouse database with ClickHouse query syntax.

Run `uv run python -m dank.tools.clickhouse_query -q "<QUERY>"` to query the
database.

You can only use `SELECT` or `SHOW` queries.

Data is truncated by default, and if you want to see more use `--full`.
