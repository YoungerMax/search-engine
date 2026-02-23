from __future__ import annotations

from app.api import main


class _FakeCursor:
    def __init__(self, rows, news_rows) -> None:
        self._rows = rows
        self._news_rows = news_rows
        self._phase = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, _sql, _params):
        self._phase += 1

    def fetchall(self):
        if self._phase == 1:
            return self._rows
        if self._phase == 2:
            return self._news_rows
        return []


class _FakeConn:
    def __init__(self, rows, news_rows) -> None:
        self._rows = rows
        self._news_rows = news_rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def cursor(self):
        return _FakeCursor(self._rows, self._news_rows)


def test_search_returns_separate_web_and_news_results(monkeypatch) -> None:
    rows = [
        ("Search title", "Search description", "https://example.com/search", 8.0, 2),
    ]
    news_rows = [
        (
            "News title",
            "News description",
            "https://example.com/news",
            "Example Feed",
            "Reporter",
            None,
            10.0,
            2,
        )
    ]

    monkeypatch.setattr(main, "tokenize", lambda _q: {"search": 1, "news": 1})
    monkeypatch.setattr(main, "get_conn", lambda: _FakeConn(rows, news_rows))

    response = main.search(q="search news", limit=20, offset=0)

    assert set(response.keys()) == {"results", "count"}
    assert set(response["results"].keys()) == {"web", "news"}
    assert len(response["results"]["web"]) == 1
    assert len(response["results"]["news"]) == 1
    assert response["count"] >= 2


def test_search_empty_query_terms_returns_expected_shape(monkeypatch) -> None:
    monkeypatch.setattr(main, "tokenize", lambda _q: {})

    response = main.search(q="the and", limit=20, offset=0)

    assert response == {"results": {"web": [], "news": []}, "count": 0}
