from app.crawler.normalization import normalize_url


def test_normalize_url_removes_fragments_and_tracking_params() -> None:
    url = "HTTPS://Example.com/path///to?p=1&utm_source=x#section"
    assert normalize_url(url) == "https://example.com/path/to?p=1"
