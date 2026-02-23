from __future__ import annotations

from app.mcp import server


def test_search_web_tool_delegates_to_api(monkeypatch) -> None:
    captured: dict[str, int | str] = {}

    def _fake_perform_web_search(*, q: str, limit: int, offset: int):
        captured["q"] = q
        captured["limit"] = limit
        captured["offset"] = offset
        return {"results": [], "count": 0}

    monkeypatch.setattr(server, "perform_web_search", _fake_perform_web_search)

    result = server.search_web(query="example", limit=999, offset=-3)

    assert result == {"results": [], "count": 0}
    assert captured == {"q": "example", "limit": 100, "offset": 0}


def test_search_news_tool_delegates_to_api(monkeypatch) -> None:
    captured: dict[str, int | str] = {}

    def _fake_perform_news_search(*, q: str, limit: int, offset: int):
        captured["q"] = q
        captured["limit"] = limit
        captured["offset"] = offset
        return {"results": [], "count": 0}

    monkeypatch.setattr(server, "perform_news_search", _fake_perform_news_search)

    result = server.search_news(query="example", limit=999, offset=-3)

    assert result == {"results": [], "count": 0}
    assert captured == {"q": "example", "limit": 100, "offset": 0}
