from app.crawler.worker import _is_feed_content_type, parse_html


def test_parse_html_extracts_feed_links() -> None:
    html = """
    <html>
      <head>
        <title>Example</title>
        <meta name='description' content='desc'/>
        <link rel='alternate' type='application/rss+xml' href='/rss.xml' />
        <link rel='alternate' type='application/atom+xml' href='https://example.com/atom.xml' />
      </head>
      <body>
        <article>%s</article>
      </body>
    </html>
    """ % ("word " * 150)

    parsed = parse_html("https://example.com/news/story", html)
    assert "https://example.com/rss.xml" in parsed.feed_links
    assert "https://example.com/atom.xml" in parsed.feed_links


def test_parse_html_extracts_feed_links_from_meta_tags() -> None:
    html = """
    <html>
      <head>
        <title>Example</title>
        <meta name='description' content='desc'/>
        <meta property='og:rss' content='https://example.com/feeds/main.xml' />
        <meta name='atom-feed' content='/atom.xml' />
      </head>
      <body>
        <article>%s</article>
      </body>
    </html>
    """ % ("word " * 150)

    parsed = parse_html("https://example.com/news/story", html)
    assert "https://example.com/feeds/main.xml" in parsed.feed_links
    assert "https://example.com/atom.xml" in parsed.feed_links


def test_is_feed_content_type() -> None:
    assert _is_feed_content_type("application/rss+xml")
    assert _is_feed_content_type("application/atom+xml; charset=utf-8")
    assert not _is_feed_content_type("text/html")
