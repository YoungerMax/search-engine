from __future__ import annotations

from app.mcp import server


def test_web_search_tool_delegates_to_api(monkeypatch) -> None:
    captured: dict[str, int | str] = {}

    def _fake_perform_search(*, q: str, limit: int, offset: int):
        captured["q"] = q
        captured["limit"] = limit
        captured["offset"] = offset
        return {"results": {"web": [], "news": []}, "count": 0}

    monkeypatch.setattr(server, "perform_search", _fake_perform_search)

    result = server.web_search(query="example", limit=999, offset=-3)

    assert result == {"results": {"web": [], "news": []}, "count": 0}
    assert captured == {"q": "example", "limit": 100, "offset": 0}
