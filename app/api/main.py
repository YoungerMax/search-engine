import os
import re
import math
from collections import defaultdict
import json
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from psycopg.errors import CharacterNotInRepertoire, UndefinedFunction, UndefinedObject

from app.common.db import get_conn
from app.crawler.tokenizer import STOPWORDS, TOKEN_RE, tokenize
from app.spellcheck.engine import (
    Candidate,
    LexiconEntry,
    WORD_RE as SPELLCHECK_WORD_RE,
    apply_case,
    choose_correction,
    normalize_word,
)

app = FastAPI(title="Search API")

SEARCH_SQL = """
WITH scored AS (
  SELECT d.id,
         d.title,
         d.description,
         d.url,
         SUM(
           t.frequency
           * COALESCE(ts.idf, 1.0)
           * CASE t.field
               WHEN 1 THEN 3.2
               WHEN 2 THEN 1.7
               ELSE 1.0
             END
         ) AS token_score,
         COUNT(DISTINCT t.term) AS matched_terms
  FROM tokens t
  JOIN documents d ON d.id = t.doc_id
  LEFT JOIN term_statistics ts ON ts.term = t.term
  WHERE d.status = 'done'
    AND t.term = ANY(%s)
  GROUP BY d.id, d.title, d.description, d.url
)
SELECT title, COALESCE(description, '') AS description, url, token_score, matched_terms
FROM scored
ORDER BY token_score DESC, url ASC
LIMIT %s
"""

FALLBACK_SEARCH_SQL = """
WITH scored AS (
  SELECT d.id,
         SUM(
           t.frequency
           * COALESCE(ts.idf, 1.0)
           * CASE t.field
               WHEN 1 THEN 3.2
               WHEN 2 THEN 1.7
               ELSE 1.0
             END
         ) AS token_score,
         COUNT(DISTINCT t.term) AS matched_terms
  FROM tokens t
  JOIN documents d ON d.id = t.doc_id
  LEFT JOIN term_statistics ts ON ts.term = t.term
  WHERE d.status = 'done'
    AND t.term = ANY(%s)
  GROUP BY d.id
)
SELECT token_score, matched_terms
FROM scored
ORDER BY token_score DESC
LIMIT %s
"""

CANDIDATE_BUFFER = 200
MAX_CANDIDATES = 2000


NEWS_SEARCH_SQL = """
SELECT na.title,
       COALESCE(na.description, '') AS description,
       na.url,
       nf.name AS feed_name,
       na.author,
       na.published_at,
       SUM(
         t.frequency
         * COALESCE(ts.idf, 1.0)
       ) AS token_score,
       COUNT(DISTINCT t.term) AS matched_terms
FROM tokens t
JOIN news_articles na ON na.url = t.article_url
LEFT JOIN news_feeds nf ON nf.feed_url = na.feed_url
LEFT JOIN term_statistics ts ON ts.term = t.term
WHERE t.source_type = 2
  AND t.term = ANY(%s)
GROUP BY na.title, na.description, na.url, nf.name, na.author, na.published_at
ORDER BY token_score DESC, na.url ASC
LIMIT %s
"""



def _normalize_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def _extract_query_words(text: str) -> list[str]:
    words: list[str] = []
    seen: set[str] = set()
    for term in TOKEN_RE.findall(text.lower()):
        if term in STOPWORDS or term in seen:
            continue
        words.append(term)
        seen.add(term)
    return words


def _count_hits(text: str, query_words: list[str]) -> int:
    if not query_words:
        return 0
    word_set = set(TOKEN_RE.findall((text or "").lower()))
    return sum(1 for word in query_words if word in word_set)


def _compact_word_hits(compact_text: str, query_words: list[str]) -> int:
    if not query_words or not compact_text:
        return 0
    return sum(1 for word in query_words if word in compact_text)


def _intent_score(
    *,
    token_score: float,
    matched_terms: int,
    total_terms: int,
    query_phrase: str,
    query_compact: str,
    query_words: list[str],
    title: str,
    description: str,
    url: str,
) -> float:
    # Damp very high baseline scores so intent signals dominate ranking.
    score = math.log1p(max(float(token_score), 0.0)) * 12.0
    if total_terms:
        coverage = matched_terms / total_terms
        score += coverage * 25.0
        if matched_terms == total_terms:
            score += 40.0

    normalized_title = _normalize_text(title)
    normalized_description = _normalize_text(description)
    normalized_url = _normalize_text(url)
    compact_url = re.sub(r"[^a-z0-9]+", "", (url or "").lower())

    if query_phrase and query_phrase in normalized_title:
        score += 140.0
    if query_phrase and query_phrase in normalized_url:
        score += 70.0
    if query_phrase and query_phrase in normalized_description:
        score += 25.0
    if query_compact and query_compact in compact_url:
        score += 90.0

    title_hits = _count_hits(title, query_words)
    url_hits = _count_hits(url, query_words)
    compact_url_hits = _compact_word_hits(compact_url, query_words)
    score += title_hits * 22.0
    score += url_hits * 16.0
    score += compact_url_hits * 12.0

    if query_words and title_hits == len(query_words):
        score += 80.0
    if query_words and url_hits == len(query_words):
        score += 55.0
    if query_words and compact_url_hits == len(query_words):
        score += 45.0

    return score


@app.get('/')
def index():
    return FileResponse(path=Path(os.path.dirname(__file__)) / 'search.html')

@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> dict[str, object]:
    return perform_search(q=q, limit=limit, offset=offset)


def perform_search(
    *,
    q: str,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, object]:
    query_terms = list(tokenize(q).keys())
    if not query_terms:
        return {"results": {"web": [], "news": []}, "count": 0}
    query_words = _extract_query_words(q)
    query_phrase = _normalize_text(q)
    query_compact = "".join(query_words)
    total_terms = len(query_terms)
    candidate_limit = min(
        MAX_CANDIDATES,
        max(offset + limit + CANDIDATE_BUFFER, limit * 10),
    )

    with get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(SEARCH_SQL, (query_terms, candidate_limit))
                rows = cur.fetchall()
                cur.execute(NEWS_SEARCH_SQL, (query_terms, candidate_limit))
                news_rows = cur.fetchall()
        except CharacterNotInRepertoire:
            # Some databases may contain legacy non-UTF8 text bytes.
            # Retry with SQL_ASCII and avoid projecting text columns entirely.
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("SET client_encoding TO SQL_ASCII")
                cur.execute(FALLBACK_SEARCH_SQL, (query_terms, candidate_limit))
                rows = cur.fetchall()
            news_rows = []
            fallback_results = []
            for row in rows:
                score = math.log1p(max(float(row[0] or 0.0), 0.0)) * 12.0
                matched_terms = int(row[1] or 0)
                if total_terms:
                    score += (matched_terms / total_terms) * 25.0
                    if matched_terms == total_terms:
                        score += 40.0
                fallback_results.append(
                    {
                        "title": "",
                        "description": "",
                        "url": "",
                        "score": score,
                    }
                )
            fallback_results.sort(key=lambda item: item["score"], reverse=True)
            fallback_page = fallback_results[offset : offset + limit]
            return {
                "results": {
                    "web": fallback_page,
                    "news": [],
                },
                "count": max(len(fallback_results), offset + len(fallback_page)),
            }

    ranked_results: list[dict[str, str | float]] = []
    for row in rows:
        title = row[0] or ""
        description = row[1] or ""
        url = row[2]
        score = _intent_score(
            token_score=float(row[3] or 0.0),
            matched_terms=int(row[4] or 0),
            total_terms=total_terms,
            query_phrase=query_phrase,
            query_compact=query_compact,
            query_words=query_words,
            title=title,
            description=description,
            url=url,
        )
        ranked_results.append(
            {
                "title": title,
                "description": description,
                "url": url,
                "score": score,
            }
        )

    ranked_results.sort(key=lambda item: (-float(item["score"]), str(item["url"])))

    news_results: list[dict[str, object]] = []
    for row in news_rows:
        title = row[0] or ""
        description = row[1] or ""
        url = row[2]
        score = _intent_score(
            token_score=float(row[6] or 0.0),
            matched_terms=int(row[7] or 0),
            total_terms=total_terms,
            query_phrase=query_phrase,
            query_compact=query_compact,
            query_words=query_words,
            title=title,
            description=description,
            url=url,
        ) + 8.0
        news_results.append(
            {
                "type": "news",
                "title": title,
                "description": description,
                "url": url,
                "score": score,
                "feed_name": row[3],
                "author": row[4],
                "published_at": row[5].isoformat() if row[5] else None,
            }
        )

    web_results = ranked_results[offset : offset + limit]
    news_page = news_results[offset : offset + limit]
    approx_count = max(len(ranked_results) + len(news_results), offset + len(web_results) + len(news_page))

    return {
        "results": {
            "web": web_results,
            "news": news_page,
        },
        "count": approx_count,
    }


SPELLCHECK_KNOWN_SQL = """
SELECT word, doc_frequency, total_frequency, external_frequency, popularity_score
FROM spellcheck_dictionary
WHERE word = ANY(%s)
"""

SPELLCHECK_CANDIDATE_SQL = """
WITH input_words AS (
  SELECT DISTINCT unnest(%s::text[]) AS input_word
)
SELECT i.input_word,
       s.word,
       s.doc_frequency,
       s.total_frequency,
       s.external_frequency,
       s.popularity_score
FROM input_words i
JOIN LATERAL (
  SELECT word, doc_frequency, total_frequency, external_frequency, popularity_score
  FROM spellcheck_dictionary
  WHERE length(word) BETWEEN GREATEST(2, length(i.input_word) - 2) AND length(i.input_word) + 2
    AND word %% i.input_word
    AND popularity_score >= %s
  ORDER BY similarity(word, i.input_word) DESC, popularity_score DESC
  LIMIT %s
) s ON TRUE
"""
SPELLCHECK_FALLBACK_SQL = """
SELECT word, doc_frequency, total_frequency, external_frequency, popularity_score
FROM spellcheck_dictionary
WHERE length(word) BETWEEN GREATEST(2, length(%s) - 2) AND length(%s) + 2
  AND left(word, 1) = left(%s, 1)
  AND popularity_score >= %s
ORDER BY popularity_score DESC
LIMIT %s
"""
SPELLCHECK_MIN_CANDIDATE_POPULARITY = 2.0
SPELLCHECK_MAX_CANDIDATES_PER_WORD = 120
SPELLCHECK_META_PATH = Path(os.environ.get("SPELLCHECK_META_PATH", "/tmp/spellcheck_meta.json"))
_spell_meta_mtime: float | None = None
_spell_meta_cache: dict[str, LexiconEntry] = {}


def _load_spell_meta() -> dict[str, LexiconEntry]:
    global _spell_meta_mtime, _spell_meta_cache
    if not SPELLCHECK_META_PATH.exists():
        return {}
    mtime = SPELLCHECK_META_PATH.stat().st_mtime
    if _spell_meta_mtime == mtime and _spell_meta_cache:
        return _spell_meta_cache
    payload = json.loads(SPELLCHECK_META_PATH.read_text())
    _spell_meta_cache = {
        row["word"]: LexiconEntry(
            word=row["word"],
            doc_frequency=int(row.get("doc_frequency", 0)),
            total_frequency=int(row.get("total_frequency", 0)),
            external_frequency=int(row.get("external_frequency", 0)),
            popularity_score=float(row.get("popularity_score", 0.0)),
        )
        for row in payload.get("words", [])
    }
    _spell_meta_mtime = mtime
    return _spell_meta_cache


@app.get("/spellcheck")
def spellcheck(
    q: str = Query(..., min_length=1),
) -> dict[str, str | None]:
    words = [normalize_word(w) for w in SPELLCHECK_WORD_RE.findall(q)]
    words = [w for w in words if w and w not in STOPWORDS]
    if not words:
        return {"suggestion": None}

    cached_lexicon = _load_spell_meta()
    known: dict[str, LexiconEntry] = {word: cached_lexicon[word] for word in words if word in cached_lexicon}

    with get_conn() as conn:
        with conn.cursor() as cur:
            missing_words = [word for word in words if word not in known]
            if missing_words:
                cur.execute(SPELLCHECK_KNOWN_SQL, (missing_words,))
                for row in cur.fetchall():
                    known[row[0]] = LexiconEntry(
                        word=row[0],
                        doc_frequency=int(row[1] or 0),
                        total_frequency=int(row[2] or 0),
                        external_frequency=int(row[3] or 0),
                        popularity_score=float(row[4] or 0.0),
                    )

            suspect: list[str] = []
            for word in words:
                entry = known.get(word)
                if entry and entry.popularity_score >= 3.0:
                    continue

                suspect.append(word)

            if not suspect:
                return {"suggestion": None}

            candidates_by_word: dict[str, dict[str, Candidate]] = defaultdict(dict)
            try:
                cur.execute(
                    SPELLCHECK_CANDIDATE_SQL,
                    (
                        suspect,
                        SPELLCHECK_MIN_CANDIDATE_POPULARITY,
                        SPELLCHECK_MAX_CANDIDATES_PER_WORD,
                    ),
                )
                for row in cur.fetchall():
                    candidate = Candidate(
                        word=row[1],
                        doc_frequency=int(row[2] or 0),
                        total_frequency=int(row[3] or 0),
                        external_frequency=int(row[4] or 0),
                        popularity_score=float(row[5] or 0.0),
                    )
                    candidates_by_word[row[0]][candidate.word] = candidate
            except (UndefinedFunction, UndefinedObject):
                conn.rollback()
                with conn.cursor() as fallback_cur:
                    for word in set(suspect):
                        fallback_cur.execute(
                            SPELLCHECK_FALLBACK_SQL,
                            (
                                word,
                                word,
                                word,
                                SPELLCHECK_MIN_CANDIDATE_POPULARITY,
                                SPELLCHECK_MAX_CANDIDATES_PER_WORD,
                            ),
                        )
                        for row in fallback_cur.fetchall():
                            candidate = Candidate(
                                word=row[0],
                                doc_frequency=int(row[1] or 0),
                                total_frequency=int(row[2] or 0),
                                external_frequency=int(row[3] or 0),
                                popularity_score=float(row[4] or 0.0),
                            )
                            candidates_by_word[word][candidate.word] = candidate

    corrected: dict[str, str] = {}
    for word in suspect:
        best = choose_correction(
            word=word,
            known=known.get(word),
            candidates=candidates_by_word.get(word, {}).values(),
        )
        if best:
            corrected[word] = best

    if not corrected:
        return {"suggestion": None}

    def _replace(match: re.Match[str]) -> str:
        token = match.group(0)
        replacement = corrected.get(token.lower())
        if not replacement:
            return token
        return apply_case(token, replacement)

    suggestion = SPELLCHECK_WORD_RE.sub(_replace, q)
    if suggestion == q:
        return {"suggestion": None}

    return {"suggestion": suggestion}
