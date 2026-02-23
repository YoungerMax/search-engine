from contextlib import contextmanager

from app.crawler.worker import _backfill_news_article_content


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


def test_backfill_updates_missing_news_content(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setattr(
        "app.crawler.worker.get_conn",
        lambda: _fake_get_conn(cursor),
    )

    _backfill_news_article_content("https://example.com/news/1", "word " * 140)

    assert len(cursor.calls) == 1
    _, params = cursor.calls[0]
    assert params[1] == "https://example.com/news/1"


def test_backfill_skips_short_content(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setattr(
        "app.crawler.worker.get_conn",
        lambda: _fake_get_conn(cursor),
    )

    _backfill_news_article_content("https://example.com/news/1", "too short")

    assert cursor.calls == []
