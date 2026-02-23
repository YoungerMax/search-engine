from __future__ import annotations

from app.api import main


def test_search_endpoint_uses_web_search(monkeypatch) -> None:
    monkeypatch.setattr(
        main,
        "perform_web_search",
        lambda *, q, limit, offset: {"results": [{"title": "Web"}], "count": 1},
    )

    response = main.search(q="search", limit=20, offset=0)

    assert response == {"results": [{"title": "Web"}], "count": 1}


def test_search_web_endpoint_uses_web_search(monkeypatch) -> None:
    monkeypatch.setattr(
        main,
        "perform_web_search",
        lambda *, q, limit, offset: {"results": [{"title": "Web only"}], "count": 1},
    )

    response = main.search_web(q="web", limit=10, offset=0)

    assert response == {"results": [{"title": "Web only"}], "count": 1}


def test_search_news_endpoint_uses_news_search(monkeypatch) -> None:
    monkeypatch.setattr(
        main,
        "perform_news_search",
        lambda *, q, limit, offset: {"results": [{"title": "News only"}], "count": 1},
    )

    response = main.search_news(q="news", limit=10, offset=0)

    assert response == {"results": [{"title": "News only"}], "count": 1}
