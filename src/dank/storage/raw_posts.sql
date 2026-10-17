ALTER TABLE raw_posts
    ADD COLUMN IF NOT EXISTS is_reference UInt8 ALIAS (
        source = 'x'
        AND JSONExtractString(payload, '__typename') = 'Tweet'
        AND JSONExtractString(payload, 'rest_id') != ''
        AND arraySort(JSONExtractKeys(payload)) = ['__typename', 'rest_id']
    );
