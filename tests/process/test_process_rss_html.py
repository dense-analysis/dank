from dank.process.page import (
    extract_article_html,
    extract_page_metadata,
    extract_youtube_iframes,
)

HTML_SINGLE_POST = """
<html>
  <head>
    <meta property="og:title" content="Sample Post" />
    <meta name="author" content="Sample Author" />
    <meta
      property="article:published_time"
      content="2026-02-01T10:52:05+00:00"
    />
    <meta name="twitter:label1" content="Written by" />
    <meta name="twitter:data1" content="Ignored Author" />
  </head>
  <body class="single single-post">
    <single-post>
      <template v-slot:content>
        <p>First line in body.</p>
        <p>Second line.</p>
      </template>
    </single-post>
  </body>
</html>
"""

HTML_ARTICLE = """
<html>
  <head>
    <meta property="og:title" content="Feature Article" />
    <script type="application/ld+json">
      {"@context":"https://schema.org","@type":"Article","author":
      {"@type":"Person","name":"Writer Name"}}
    </script>
  </head>
  <body>
    <main>
      <article>
        <div class="entry-content">
          <p>Intro paragraph.</p>
          <img src="https://cdn.example.test/image.jpg" />
          <iframe src="https://www.youtube.com/embed/abc123"></iframe>
        </div>
      </article>
    </main>
  </body>
</html>
"""

HTML_TWITTER_AUTHOR = """
<html>
  <head>
    <meta property="og:title" content="Meta Title" />
    <meta name="twitter:label1" content="Author" />
    <meta name="twitter:data1" content="Twitter Author" />
  </head>
  <body><p>Body</p></body>
</html>
"""


def test_extract_article_html_single_post() -> None:
    article_html = extract_article_html(HTML_SINGLE_POST)

    assert "First line in body." in article_html
    assert "<single-post" not in article_html


def test_extract_article_html_article_tag() -> None:
    article_html = extract_article_html(HTML_ARTICLE)

    assert "https://cdn.example.test/image.jpg" in article_html
    assert "youtube.com/embed/abc123" in article_html


def test_extract_page_metadata_author_from_meta() -> None:
    metadata = extract_page_metadata(HTML_SINGLE_POST)

    assert metadata.author == "Sample Author"


def test_extract_page_metadata_author_from_jsonld() -> None:
    metadata = extract_page_metadata(HTML_ARTICLE)

    assert metadata.author == "Writer Name"


def test_extract_youtube_iframes() -> None:
    iframe_html = extract_youtube_iframes(HTML_ARTICLE)

    assert "youtube.com/embed/abc123" in iframe_html


def test_extract_page_metadata_author_from_twitter_label() -> None:
    metadata = extract_page_metadata(HTML_TWITTER_AUTHOR)

    assert metadata.author == "Twitter Author"
