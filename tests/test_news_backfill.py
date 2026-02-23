import asyncio
from contextlib import asynccontextmanager

from app.crawler.worker import _backfill_news_article_content


class _FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))


class _CursorCtx:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    async def __aenter__(self):
        return self._cursor

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self):
        return _CursorCtx(self._cursor)


@asynccontextmanager
async def _fake_get_conn(cursor: _FakeCursor):
    yield _FakeConn(cursor)


def test_backfill_updates_missing_news_content(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setattr("app.crawler.worker.get_conn", lambda: _fake_get_conn(cursor))

    asyncio.run(_backfill_news_article_content("https://example.com/news/1", "word " * 140))

    assert len(cursor.calls) == 1
    _, params = cursor.calls[0]
    assert params[1] == "https://example.com/news/1"


def test_backfill_skips_short_content(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setattr("app.crawler.worker.get_conn", lambda: _fake_get_conn(cursor))

    asyncio.run(_backfill_news_article_content("https://example.com/news/1", "too short"))

    assert cursor.calls == []
