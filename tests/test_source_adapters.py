from pathlib import Path

import pytest

from dank.config import ConfigError, SourceAdapter, load_settings


def source(tmp_path: Path, text: str):
    path = tmp_path / "adapters.fixture.toml"
    path.write_text(f"sources = [{" ".join(text.split())}]")

    return load_settings(path).sources[0]


def test_hn_explicit_feed_works_as_an_ordinary_rss_source(tmp_path: Path):
    result = source(tmp_path, '{domain="news.ycombinator.com", '
                    'feed_urls=["https://news.ycombinator.com/rss"]}')
    assert result.domain == "news.ycombinator.com"
    assert [item.kind for item in result.collection_adapters] == ["rss"]
    assert result.collection_adapters[0].feed_urls == (
        "https://news.ycombinator.com/rss",
    )


def test_hn_composes_existing_collector_and_rss(tmp_path: Path):
    result = source(tmp_path, '''{domain="hckrnews.com", tags=["Tech"],
        hacker_news_filter="all", adapters=[
            {type="hacker_news"},
            {type="rss", feed_urls=["https://news.ycombinator.com/rss"]}
        ]}''')
    assert result.domain == "news.ycombinator.com"
    assert result.tags == ("tech",)
    assert result.hacker_news and result.hacker_news.filter == "all"
    assert [item.kind for item in result.collection_adapters] == [
        "hacker_news", "rss",
    ]


def test_multiple_rss_adapters_keep_their_own_feed_urls(tmp_path: Path):
    result = source(tmp_path, '''{domain="publisher.test", adapters=[
        {type="rss", feed_urls=["https://provider.test/news.xml"]},
        {type="rss", feed_urls=["https://provider.test/tech.xml"]}
    ]}''')
    assert [item.feed_urls for item in result.collection_adapters] == [
        ("https://provider.test/news.xml",),
        ("https://provider.test/tech.xml",),
    ]


@pytest.mark.parametrize("domain,expected", [
    ("publisher.test", "rss"), ("hckrnews.com", "hacker_news"),
    ("x.com", "x"), ("youtube.com", "youtube"),
])
def test_legacy_defaults_remain_unchanged(
    tmp_path: Path, domain: str, expected: str,
):
    result = source(tmp_path, f'"{domain}"')
    assert result.collection_adapters[0].kind == SourceAdapter(expected)


@pytest.mark.parametrize("adapters", [
    '[]', '"rss"', '["rss"]', '[{}]', '[{type="made_up"}]',
    '[{type="rss", typo=true}]', '[{type="rss", feed_urls=["/rss"]}]',
    '[{type="rss"}, {type="rss"}]', '[{type="youtube"}]',
    '[{type="hacker_news", feed_urls=["https://example.test/"]}]',
])
def test_invalid_adapters_fail_before_collection(
    tmp_path: Path, adapters: str,
):
    with pytest.raises(ConfigError):
        source(tmp_path, f'{{domain="hckrnews.com", adapters={adapters}}}')
