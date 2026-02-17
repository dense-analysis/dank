from dank.scrape.x.payloads import (
    _iter_timeline_tweet_results,  # pyright: ignore[reportPrivateUsage]
)


def test_iter_timeline_tweet_results_extracts_entries() -> None:
    tweet_a = {"rest_id": "111", "legacy": {"full_text": "hello"}}
    tweet_b = {"rest_id": "222", "legacy": {"full_text": "world"}}
    module_item: dict[str, object] = {
        "item": {
            "itemContent": {
                "tweet_results": {
                    "result": {
                        "tweet": tweet_b,
                    },
                },
            },
        },
    }

    payload: dict[str, object] = {
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
                                                "entryType": (
                                                    "TimelineTimelineItem"
                                                ),
                                                "itemContent": {
                                                    "itemType": (
                                                        "TimelineTweet"
                                                    ),
                                                    "tweet_results": {
                                                        "result": tweet_a,
                                                    },
                                                },
                                            },
                                        },
                                        {
                                            "content": {
                                                "entryType": (
                                                    "TimelineTimelineModule"
                                                ),
                                                "items": [
                                                    module_item,
                                                ],
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

    extracted = _iter_timeline_tweet_results(payload)

    assert [tweet.get("rest_id") for tweet in extracted] == ["111", "222"]


def test_iter_timeline_tweet_results_ignores_non_add_entries() -> None:
    payload: dict[str, object] = {
        "data": {
            "user": {
                "result": {
                    "timeline": {
                        "timeline": {
                            "instructions": [
                                {
                                    "type": "TimelinePinEntry",
                                    "entries": [],
                                },
                            ],
                        },
                    },
                },
            },
        },
    }

    extracted = _iter_timeline_tweet_results(payload)

    assert extracted == []


def test_iter_timeline_tweet_results_returns_empty_for_missing_path() -> None:
    extracted = _iter_timeline_tweet_results({"data": {"user": {}}})

    assert extracted == []
