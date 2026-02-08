Run `uv run python -m dank.tools.embed_text 'your text'` to print embeddings.

This is a quick way to check what the embeddings for text will be during
analysis using the same model that DANK uses for embeddings stored in the
database.

You can run ClickHouse queries with cosine distance to compare embeddings in the
database with the QUERY tool (`dank.tools.clickhouse_query`).
