ALTER TABLE posts ADD COLUMN IF NOT EXISTS embedding_algorithm LowCardinality(String) DEFAULT '';
ALTER TABLE posts ADD COLUMN IF NOT EXISTS categories Array(Tuple(name String, confidence Float32, similarity Float32)) DEFAULT [];

CREATE TABLE IF NOT EXISTS category_training (
    embedding Array(Float32) CODEC(ZSTD(3)),
    embedding_algorithm LowCardinality(String),
    category LowCardinality(String),
    confidence Float64,
    CONSTRAINT valid_confidence CHECK isFinite(confidence) AND confidence >= 0 AND confidence <= 1,
    CONSTRAINT valid_embedding CHECK notEmpty(embedding) AND arrayAll(x -> isFinite(x), embedding) AND arraySum(arrayMap(x -> x * x, embedding)) > 0
)
ENGINE = MergeTree
ORDER BY (embedding_algorithm, category);
