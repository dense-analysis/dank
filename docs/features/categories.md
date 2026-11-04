# Category training and assignment

`make train MODE=native ARGS='--age 24h --limit 100'` (or `uv run train`)
sends saved processed post URL, author, title and HTML
to OpenAI's [Decisions API](https://developers.openai.com/api/docs/guides/decisions).
Each category becomes an independent predicate question over `input_json`
evidence. The returned predicate probability is stored as confidence.
Docker users omit `MODE=native`. Configure your private `config.toml`:

```toml
[categories]
enabled = true
api_key = "YOUR_OPENAI_API_KEY"
model = "gpt-6-luna"
cosine_threshold = 0.85
confidence_threshold = 0.8
concurrency = 4
max_content_chars = 8192

[[categories.definitions]]
name = "software"
description = "Software development, programming tools and languages."

[[categories.definitions]]
name = "science"
description = "Scientific research and discoveries, including space exploration."
```

`enabled` controls assignment in `process`; training is always an explicit
command and requires category definitions and an API key. Processing uses
only local ClickHouse queries and needs no OpenAI key. Defaults for thresholds
are project heuristics: tune them against reviewed examples. API probabilities are
model estimates; validate their calibration against reviewed examples. Categories are independent;
a post can match several or none.

Training selects configured domains, optionally filtered with `--domains`
(a regex), and posts updated within `--age` (default `24h`, or `all`).
`--limit` is a positive maximum number of posts newly labelled per invocation
(default 10,000), including posts with partially missing category scores.
Decisions currently supports only `gpt-6-luna`, and configuration accepts up
to 400 categories per request. JSON evidence must fit the API limits of
256 KiB. The four string fields stay below its 10,000-value limit.
It pages through saved posts in domain/ID order, skipping examples whose
vector, algorithm and category already exist. Requests run with bounded
concurrency and retry transient network errors, HTTP 429 and 5xx up to three
attempts. Successful sibling requests are persisted before a failure ends the
command with nonzero status. Explicit per-category refusals are logged and omitted while valid sibling scores
are saved; even a fully refused post counts toward the invocation limit. Refused
categories are not saved as zero and may be requested again on a later run.
Missing or duplicate answers, malformed scores,
missing categories, nonfinite values and values outside [0, 1] cause that post to be skipped.

The JSON input is a `PostTrainingData` TypedDict with exactly `url`, `author`,
`title` (up to 512 characters), and `html` (up to `max_content_chars`). HTML
markup is preserved. Embeddings and their algorithm names stay local and
are used only to select and persist training examples. The Decisions prompt
explicitly requests independent confidence between 0 and 1 for each category,
returned as predicate probability. Post fields are framed as untrusted data
in a developer message. HTTP status, bounded request IDs, retry delays and refusal counts are logged.
Input, refusal text and remote error bodies are not logged. HTTP 429 follows
the bounded retry path; a refusal in HTTP 200 is a separate outcome and does
not identify a particular safety policy. Exhausted retries still stop the run.
Invalid evidence or malformed answers
are logged and skipped per post, counting toward the invocation limit.
Training incurs API charges; DANK does not fine-tune an OpenAI model.

Training orchestration, the CLI and Decisions requests live in `dank.training`.
Only `dank.training.runner` accesses ClickHouse there. Processing loads and
assigns categories locally; it cannot import training. Shared post TypedDicts,
vector selection and duration parsing have independent modules with no
imports from the scraping, processing or training pipelines. `AGENTS.md` and
`run-linters.sh` enforce those boundaries.

## Reader

The reader displays saved category names on post cards and article pages.
Choose one category below the platform selector; choosing another replaces
the current selection. Repeated `category` URL parameters still match with OR,
and category filters combine with the other filters using AND. **All categories** removes the
category filter. The URL preserves selections through reload and article
navigation. Unassigned posts appear when no category is selected.

`GET /api/reader/categories` returns `{ "categories": ["science"] }` from
`categories.definitions` in the loaded `config.toml`, without querying the
database. Removed category names are not offered, while existing post labels
remain saved. Feed and detail responses include `categories` objects with
`name`, `confidence`, and `similarity`; repeated `category` query parameters
filter the feed by exact, case-sensitive names. Confidence and similarity
remain saved example evidence, not predicted probabilities for the post.

## Prompt caching

Category names and descriptions appear only in the predicate questions. The
shared input contains stable developer instructions followed by the post JSON.
A SHA-256 cache key derived from the instructions stays stable across category
changes. Decisions' default implicit breakpoint at the end of shared input
can reuse identical post evidence on retries or when asking for missing labels.

[Decisions prompt caching](https://developers.openai.com/api/docs/guides/decisions#reuse-shared-input-with-prompt-caching)
requires at least 1,024 tokens in the matching shared prefix; questions do not
count. The short developer instructions alone do not reach this threshold, so
cross-post reuse is not expected. Repeated evidence can qualify when it is long
enough. DANK does not pad prompts. Actual hits depend on OpenAI cache state;
there has been no live measurement. Cache reads and writes are reported under
`usage.input_tokens_details`.

Newly processed posts record `embedding_algorithm`. Old posts get an empty
algorithm when the schema upgrades and can be trained without reprocessing;
training preserves that empty value in its examples.
The selected vector is the body embedding when readable body text exists,
otherwise the title embedding. Empty, nonfinite and zero vectors are skipped.
The same choice is used for training and assignment.

`category_training` stores exactly four fields: `embedding Array(Float32)`,
`embedding_algorithm LowCardinality(String)`, `category LowCardinality(String)`
and `confidence Float64`. Training skips zero-confidence scores and retains
all positive scores, including those below `confidence_threshold`. An evaluated
post counts toward `--limit` even if all its scores are zero. Skipped zero
scores are not recorded, so those categories may be requested again on a later
training run. Classification only considers examples meeting
`confidence_threshold`. Per category, a qualifying
example must also have cosine similarity >= `cosine_threshold`. DANK chooses
the most similar eligible example (higher confidence breaks ties), and saves
its confidence and similarity in the processed post's `categories` array.
These are example confidence and measured similarity, not a predicted
probability for the new post. Vector dimensions must match; algorithm names
are metadata and do not filter assignment. Blank-algorithm training examples
can therefore classify newly reprocessed posts with named algorithms. Vectors
of the same dimension are compared even when algorithm names differ.

Assignments are saved with each processed post, so a reprocess replaces old
assignments, including with an empty array. Training does not automatically
reclassify existing posts: run `process --reprocess` to apply new examples or
thresholds. Disabling assignment writes empty categories on reprocessed posts.
Use a new category name when changing its meaning; existing labels are not
invalidated by editing a description. Training examples intentionally carry
no post identity or classifier provenance. Simultaneous train invocations may
append duplicate examples; they do not change max-similarity classification.

## ClickHouse representation and query research

The initial implementation uses exact `1 - cosineDistance` radius queries,
filtered by active category names, dimension and confidence. The MergeTree
sort key `(embedding_algorithm, category)` groups examples by provenance and
category; assignment does not restrict the leading algorithm key.
The result groups by category without a top-k limit, retaining every category
with a qualifying example. Queries run with bounded concurrency; they scan
eligible examples for every new post. This trades speed at large training
sizes for predictable threshold decisions and modest local datasets.

[ClickHouse vector search documentation](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/annindexes)
describes brute-force search and HNSW `vector_similarity` skip indexes.
HNSW accelerates `ORDER BY distance LIMIT k` searches, with a graph held in
memory. It can miss neighbors and categories; post-filtering can return fewer
than k results. An ANN top-k query cannot guarantee finding every category
above a radius threshold. Any future ANN candidate stage needs recall tests
against exact classification, per-category coverage, and exact rescoring.

Keep canonical vectors as Float32 with ZSTD compression. For the default
384-dimensional model, raw vector payload is 1,536 bytes per example; one
million examples are about 1.54 GB before compression and other columns.
The same vector is stored once per category, so multiply by category count.
Normalisation permits dot-product comparisons, but cosine distance also
handles non-unit vectors without relying on that invariant.

A normalised design could store each vector once and keep category/confidence
labels in a separate relation, or an array of label tuples. That reduces the
vector duplication cost as category count grows, but adds a join or array
expansion to filtered searches. The shipped table follows the requested
four-field example layout. Deduplicating vectors is a separate schema
tradeoff to measure before introducing that extra query work.

The documented HNSW quantisation parameter changes the graph's vector
representation independently of the source column. Supported options vary
by server version; the current documentation lists f64, f32, f16, bf16, i8
and b1, with bf16 as default. Index quantisation plus original-vector rescoring
is the preferable future experiment. BFloat16 source columns halve vector
bytes but permanently lose precision; int8 and binary quantisation lose more
information and need scale/normalisation care. Small errors near a threshold
can flip labels. No lossy quantisation is enabled without measured agreement
on reviewed post/category examples. An index also needs a fixed dimension;
mixed algorithms/dimensions need separate indexes/tables or fixed-dimension
projections. The current table deliberately supports multiple algorithms.

## Verification

Category checks live in `tests/training/`, `tests/process/test_categories.py`,
`tests/test_categories.py`, and `tests/test_lint_architecture.py`. They cover
probability validation and retries, input limits, category query filters and
bindings, returned assignments, resumable training, request limits, retained
writes during a sibling failure, the four-field HTML payload, stable cache
prefixes and pipeline import boundaries. Storage uses in-memory fakes and
Decisions replies are simulated. Run these checks with `make test`; no active
ClickHouse server or `--clickhouse-url` is needed. These unit tests exercise
query construction and orchestration, rather than executing ClickHouse SQL.
No live paid API call or cache-hit measurement was made.
[DEN-69](https://linear.app/dense-analysis/issue/DEN-69/benchmark-category-accuracy-and-clickhouse-vector-quantisation)
tracks corpus calibration and performance/quantisation measurements.

Reader checks are in `tests/web/test_reader_categories.py`,
`tests/api/sources/test_categories.py`, `reader/src/lib/categories.test.ts`,
`reader/src/lib/api.test.ts`, and `reader/tests/categories.spec.ts`. They cover
exact-name bindings, cursor isolation, named/positional ClickHouse tuple
payloads, catalog validation, selection persistence and article navigation at
320px, 390px and desktop widths. Browser verification also exercised saved
assignments on a local ClickHouse 26.6 server: multi-category URLs, single-category controls, platform
intersection and empty results, article return, and filtered cursor pagination
through both API servers. This used existing collected data without collection,
training or reprocessing.
