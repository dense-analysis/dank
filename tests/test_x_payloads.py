import datetime

from dank.scrape.x_payloads import extract_posts_from_payload


def test_extract_posts_from_payload() -> None:
    tweet_result = {
        "__typename": "Tweet",
        "rest_id": "123",
        "core": {
            "user_results": {"result": {"legacy": {"screen_name": "alice"}}},
        },
        "legacy": {
            "created_at": "Tue Jan 27 23:56:27 +0000 2026",
            "full_text": "Hello world",
            "entities": {
                "urls": [{"expanded_url": "https://example.com"}],
                "media": [
                    {
                        "media_url_https": "https://pbs.twimg.com/media/abc.jpg",
                        "type": "photo",
                    },
                ],
            },
            "extended_entities": {
                "media": [
                    {
                        "media_url_https": "https://pbs.twimg.com/media/vid.jpg",
                        "type": "video",
                        "video_info": {
                            "variants": [
                                {
                                    "content_type": "video/mp4",
                                    "url": "https://video.example.com/vid.mp4",
                                },
                            ],
                        },
                    },
                ],
            },
        },
    }
    payload = {
        "data": {
            "user": {
                "result": {
                    "timeline": {
                        "timeline": {
                            "instructions": [
                                {
                                    "type": "TimelineAddEntries",
                                    "entries": [
                                        {
                                            "content": {
                                                "itemContent": {
                                                    "tweet_results": {
                                                        "result": tweet_result,
                                                    },
                                                },
                                            },
                                        },
                                    ],
                                },
                            ],
                        },
                    },
                },
            },
        },
    }

    posts = extract_posts_from_payload(payload)
    assert len(posts) == 1
    post = posts[0]
    assert post.post_id == "123"
    assert post.author == "alice"
    assert post.url.endswith("/status/123")
    assert post.created_at == datetime.datetime(
        2026, 1, 27, 23, 56, 27, tzinfo=datetime.UTC,
    )
    asset_urls = {asset.url for asset in post.assets}
    assert "https://example.com" in asset_urls
    assert "https://pbs.twimg.com/media/abc.jpg" in asset_urls
    assert "https://video.example.com/vid.mp4" in asset_urls
