from dank.scrape.rss import _extract_page_assets  # type: ignore

HTML_WITH_ASSETS = """
<html>
  <body>
    <article>
      <img src="/img.png" />
      <video src="video.mp4"><source src="alt.mp4" /></video>
      <audio><source src="audio.mp3" /></audio>
      <iframe src="https://www.youtube.com/embed/abc123"></iframe>
      <iframe src="https://player.vimeo.com/video/999"></iframe>
    </article>
  </body>
</html>
"""


def test_extract_page_assets_from_article() -> None:
    assets = _extract_page_assets(
        HTML_WITH_ASSETS,
        domain="example.test",
        post_id="post-id",
        base_url="https://news.example.test/post",
    )
    urls = {asset.url for asset in assets}

    assert any(asset.asset_type == "youtube" for asset in assets)
    assert any("https://news.example.test/img.png" == url for url in urls)


def test_extract_page_assets_media_tags() -> None:
    assets = _extract_page_assets(
        HTML_WITH_ASSETS,
        domain="example.test",
        post_id="post-id",
        base_url="https://news.example.test/post",
    )
    urls = {asset.url: asset.asset_type for asset in assets}

    assert urls["https://news.example.test/img.png"] == "image"
    assert urls["https://news.example.test/video.mp4"] == "video"
    assert urls["https://news.example.test/alt.mp4"] == "video"
    assert urls["https://news.example.test/audio.mp3"] == "audio"
    assert urls["https://player.vimeo.com/video/999"] == "iframe"
