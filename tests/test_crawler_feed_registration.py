import asyncio
from contextlib import asynccontextmanager

from app.crawler.worker import _register_feed_url


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


def test_register_feed_url_upserts_feed(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setattr("app.crawler.worker.get_conn", lambda: _fake_get_conn(cursor))

    asyncio.run(_register_feed_url("https://example.com/feed.xml"))

    assert len(cursor.calls) == 1
    sql, params = cursor.calls[0]
    assert "INSERT INTO news_feeds" in sql
    assert params == (
        "https://example.com/feed.xml",
        "https://example.com/feed.xml",
        "https://example.com/feed.xml",
    )
