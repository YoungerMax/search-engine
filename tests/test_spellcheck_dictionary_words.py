from app.batch.spellcheck_dictionary import _collect_word_stats


class _FakeCursor:
    def __init__(self) -> None:
        self.executed_sql: str | None = None
        self._rows = [
            ("running", 11),
            ("cats", 7),
        ]

    def execute(self, sql: str) -> None:
        self.executed_sql = sql

    def __iter__(self):
        return iter(self._rows)


def test_collect_word_stats_uses_words_table() -> None:
    cur = _FakeCursor()

    doc_freq, total_freq = _collect_word_stats(cur)

    assert cur.executed_sql is not None
    assert "FROM words" in cur.executed_sql
    assert doc_freq["running"] == 0
    assert total_freq["running"] == 11
    assert doc_freq["cats"] == 0
    assert total_freq["cats"] == 7
