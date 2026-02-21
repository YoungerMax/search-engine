import os
import re
import math
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from psycopg.errors import CharacterNotInRepertoire

from app.common.db import get_conn
from app.crawler.tokenizer import STOPWORDS, TOKEN_RE, tokenize

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
) -> dict[str, list[dict[str, str | float]]]:
    query_terms = list(tokenize(q).keys())
    if not query_terms:
        return {"results": []}
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
        except CharacterNotInRepertoire:
            # Some databases may contain legacy non-UTF8 text bytes.
            # Retry with SQL_ASCII and avoid projecting text columns entirely.
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("SET client_encoding TO SQL_ASCII")
                cur.execute(FALLBACK_SEARCH_SQL, (query_terms, candidate_limit))
                rows = cur.fetchall()
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
            return {
                "results": fallback_results[offset : offset + limit]
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

    return {
        "results": ranked_results[offset : offset + limit]
    }
