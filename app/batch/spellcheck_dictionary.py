import asyncio
import json
import logging
import math
import os
import urllib.request
from datetime import datetime
from pathlib import Path
from collections import Counter
from dataclasses import dataclass

from app.common.db import get_conn
from app.spellcheck.engine import normalize_word, popularity_score

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_S = 8

SPELLCHECK_META_PATH = os.environ.get("SPELLCHECK_META_PATH", "/tmp/spellcheck_meta.json")
SPELLCHECK_META_MAX_WORDS = int(os.environ.get("SPELLCHECK_META_MAX_WORDS", "120000"))


@dataclass(frozen=True)
class ExternalSource:
    name: str
    url: str
    mode: str
    limit: int
    weight: float


EXTERNAL_SOURCES = (
    ExternalSource(
        name="frequencywords-50k",
        url="https://raw.githubusercontent.com/hermitdave/FrequencyWords/master/content/2018/en/en_50k.txt",
        mode="counted",
        limit=50000,
        weight=1.0,
    ),
    ExternalSource(
        name="google-20k",
        url="https://raw.githubusercontent.com/first20hours/google-10000-english/master/20k.txt",
        mode="ranked",
        limit=20000,
        weight=1.0,
    ),
)


def _fetch_lines(source: ExternalSource):
    req = urllib.request.Request(
        source.url,
        headers={"User-Agent": "search-engine-spellcheck/1.0"},
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_S) as resp:
        for raw_line in resp:
            line = raw_line.decode("utf-8", errors="ignore").strip()
            if line:
                yield line


def _parse_counted_line(line: str) -> tuple[str, int] | None:
    parts = line.split()
    if len(parts) < 2:
        return None

    word = normalize_word(parts[0])
    if not word.isalpha() or len(word) < 2:
        return None

    count_token = parts[1].replace(",", "")
    if not count_token.isdigit():
        return None

    return word, int(count_token)


def _collect_external_frequencies() -> Counter[str]:
    external_frequency: Counter[str] = Counter()

    for source in EXTERNAL_SOURCES:
        loaded = 0
        try:
            for rank, line in enumerate(_fetch_lines(source), start=1):
                if loaded >= source.limit:
                    break

                if source.mode == "counted":
                    parsed = _parse_counted_line(line)
                    if parsed is None:
                        continue
                    word, raw_count = parsed
                    score = int(math.log1p(raw_count) * 6.0 * source.weight)
                else:
                    word = normalize_word(line.split()[0])
                    if not word.isalpha() or len(word) < 2:
                        continue
                    score = int(math.log1p(max(1, source.limit - rank + 1)) * 5.0 * source.weight)

                if score <= 0:
                    continue

                external_frequency[word] += score
                loaded += 1

            logger.info("loaded %s external words from %s", loaded, source.name)
        except Exception:
            logger.exception("failed to load external words from %s", source.url)

    return external_frequency


async def _rebuild_words_table(cur) -> None:
    await cur.execute(
        """
        CREATE TABLE IF NOT EXISTS words (
            word TEXT PRIMARY KEY,
            total_frequency BIGINT NOT NULL
        )
        """
    )

    await cur.execute("TRUNCATE TABLE words")
    await cur.execute(
        """
        INSERT INTO words(word, total_frequency)
        SELECT word, SUM(freq) AS total_frequency
        FROM (
            SELECT m.word AS word, COUNT(*)::bigint AS freq
            FROM documents d
            JOIN LATERAL regexp_matches(lower(
                concat_ws(' ', d.title, d.description, d.content)
            ), '[a-z]{2,32}', 'g') AS m(word) ON TRUE
            WHERE d.status = 'done'
            GROUP BY m.word

            UNION ALL

            SELECT m.word AS word, COUNT(*)::bigint AS freq
            FROM news_articles na
            JOIN LATERAL regexp_matches(lower(
                concat_ws(' ', na.title, na.description, na.content)
            ), '[a-z]{2,32}', 'g') AS m(word) ON TRUE
            GROUP BY m.word
        ) all_words
        GROUP BY word
        """
    )


async def _collect_word_stats(cur) -> tuple[Counter[str], Counter[str]]:
    doc_frequency: Counter[str] = Counter()
    total_frequency: Counter[str] = Counter()

    await cur.execute(
        """
        SELECT word, total_frequency
        FROM words
        """
    )

    for word, total_freq in await cur.fetchall():
        normalized = normalize_word(word)
        if not normalized.isalpha() or len(normalized) < 2:
            continue
        total_frequency[normalized] += int(total_freq or 0)

    return doc_frequency, total_frequency


async def run() -> None:
    external_frequency = _collect_external_frequencies()

    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await _rebuild_words_table(cur)
            doc_frequency, total_frequency = await _collect_word_stats(cur)

            all_words = set(doc_frequency.keys()) | set(total_frequency.keys()) | set(external_frequency.keys())
            dictionary_rows: list[tuple[str, int, int, int, float]] = []

            for word in all_words:
                if len(word) < 2 or len(word) > 32 or not word.isalpha():
                    continue

                doc_freq = int(doc_frequency.get(word, 0))
                total_freq = int(total_frequency.get(word, 0))
                ext_freq = int(external_frequency.get(word, 0))

                if doc_freq == 0 and total_freq == 0 and ext_freq == 0:
                    continue

                pop = popularity_score(doc_freq, total_freq, ext_freq)
                dictionary_rows.append((word, doc_freq, total_freq, ext_freq, pop))

            if not dictionary_rows:
                logger.warning("spellcheck dictionary rebuild skipped: no words collected")
                return

            dictionary_rows.sort(key=lambda row: row[4], reverse=True)
            _write_meta_file(dictionary_rows)

            await cur.execute(
                """
                CREATE TEMP TABLE tmp_spellcheck_dictionary (
                    word TEXT PRIMARY KEY,
                    doc_frequency BIGINT NOT NULL,
                    total_frequency BIGINT NOT NULL,
                    external_frequency BIGINT NOT NULL,
                    popularity_score DOUBLE PRECISION NOT NULL
                ) ON COMMIT DROP
                """
            )

            async with cur.copy(
                """
                COPY tmp_spellcheck_dictionary(
                    word,
                    doc_frequency,
                    total_frequency,
                    external_frequency,
                    popularity_score
                ) FROM STDIN
                """
            ) as copy:
                for row in dictionary_rows:
                    await copy.write_row(row)

            await cur.execute(
                """
                INSERT INTO spellcheck_dictionary(
                    word,
                    doc_frequency,
                    total_frequency,
                    external_frequency,
                    popularity_score
                )
                SELECT
                    word,
                    doc_frequency,
                    total_frequency,
                    external_frequency,
                    popularity_score
                FROM tmp_spellcheck_dictionary
                ON CONFLICT (word) DO UPDATE
                SET
                    doc_frequency = EXCLUDED.doc_frequency,
                    total_frequency = EXCLUDED.total_frequency,
                    external_frequency = EXCLUDED.external_frequency,
                    popularity_score = EXCLUDED.popularity_score
                WHERE
                    spellcheck_dictionary.doc_frequency IS DISTINCT FROM EXCLUDED.doc_frequency
                    OR spellcheck_dictionary.total_frequency IS DISTINCT FROM EXCLUDED.total_frequency
                    OR spellcheck_dictionary.external_frequency IS DISTINCT FROM EXCLUDED.external_frequency
                    OR spellcheck_dictionary.popularity_score IS DISTINCT FROM EXCLUDED.popularity_score
                """
            )
            upserted_rows = cur.rowcount

            await cur.execute(
                """
                DELETE FROM spellcheck_dictionary s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM tmp_spellcheck_dictionary t
                    WHERE t.word = s.word
                )
                """
            )
            deleted_rows = cur.rowcount

            logger.info(
                "synced spellcheck dictionary: source_words=%s changed_rows=%s removed_rows=%s",
                len(dictionary_rows),
                upserted_rows,
                deleted_rows,
            )


def _write_meta_file(dictionary_rows: list[tuple[str, int, int, int, float]]) -> None:
    top_rows = dictionary_rows[:SPELLCHECK_META_MAX_WORDS]
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "words": [
            {
                "word": word,
                "doc_frequency": doc_freq,
                "total_frequency": total_freq,
                "external_frequency": ext_freq,
                "popularity_score": pop,
            }
            for word, doc_freq, total_freq, ext_freq, pop in top_rows
        ],
    }
    path = Path(SPELLCHECK_META_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


if __name__ == "__main__":
    asyncio.run(run())
