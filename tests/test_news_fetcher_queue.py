from contextlib import contextmanager

from app.batch.news_fetcher import _persist_feed


class _FakeCursor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        return None

    def executemany(self, sql: str, seq) -> None:
        self.executemany_calls.append((sql, list(seq)))


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    @contextmanager
    def cursor(self):
        yield self._cursor


@contextmanager
def _fake_get_conn(cursor: _FakeCursor):
    yield _FakeConn(cursor)


def test_persist_feed_enqueues_article_urls(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setattr("app.batch.news_fetcher.get_conn", lambda: _fake_get_conn(cursor))

    items = [
        {
            "url": "https://example.com/news/a",
            "title": "Breaking market update",
            "description": "Stocks rally strongly",
            "content": "",
            "author": "",
            "published_at": None,
        },
        {
            "url": "https://example.com/news/b",
            "title": "Tech earnings surge",
            "description": "Revenue growth outlook",
            "content": "",
            "author": "",
            "published_at": None,
        },
    ]

    _persist_feed("https://example.com/feed.xml", items)

    queue_calls = [call for call in cursor.executemany_calls if "INSERT INTO crawl_queue" in call[0]]
    assert len(queue_calls) == 1
    rows = queue_calls[0][1]
    urls = {row[0] for row in rows}
    assert urls == {"https://example.com/news/a", "https://example.com/news/b"}

    token_calls = [call for call in cursor.executemany_calls if "INSERT INTO tokens" in call[0]]
    assert len(token_calls) == 2
    assert all("source_type" in call[0] for call in token_calls)
