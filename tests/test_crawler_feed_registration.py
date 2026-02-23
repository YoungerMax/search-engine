from contextlib import contextmanager

from app.crawler.worker import _register_feed_url


class _FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.calls.append((sql, params))


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    @contextmanager
    def cursor(self):
        yield self._cursor


@contextmanager
def _fake_get_conn(cursor: _FakeCursor):
    yield _FakeConn(cursor)


def test_register_feed_url_upserts_feed(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setattr("app.crawler.worker.get_conn", lambda: _fake_get_conn(cursor))

    _register_feed_url("https://example.com/feed.xml")

    assert len(cursor.calls) == 1
    sql, params = cursor.calls[0]
    assert "INSERT INTO news_feeds" in sql
    assert params == (
        "https://example.com/feed.xml",
        "https://example.com/feed.xml",
        "https://example.com/feed.xml",
    )
