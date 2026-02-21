from app.crawler.normalization import normalize_url, registrable_domain


def test_normalize_url_removes_fragments_and_tracking_params() -> None:
    url = "HTTPS://Example.com/path///to?p=1&utm_source=x#section"
    assert normalize_url(url) == "https://example.com/path/to?p=1"


def test_registrable_domain_collapses_subdomains() -> None:
    assert registrable_domain("https://staff.blog.tumblr.com/post/123") == "tumblr.com"
    assert registrable_domain("https://www.tumblr.com/explore") == "tumblr.com"


def test_registrable_domain_handles_common_multipart_suffix() -> None:
    assert registrable_domain("https://a.bbc.co.uk/news") == "bbc.co.uk"
