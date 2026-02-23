from __future__ import annotations

import math
import re
from typing import Any

from pydantic import BaseModel, Field
from psycopg.errors import CharacterNotInRepertoire

from app.common.db import get_conn
from app.crawler.tokenizer import STOPWORDS, TOKEN_RE, tokenize

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

CANDIDATE_BUFFER = 200
MAX_CANDIDATES = 2000


class WebSearchItem(BaseModel):
    title: str
    description: str
    url: str
    score: float


class NewsSearchItem(BaseModel):
    type: str = Field(default="news")
    title: str
    description: str
    url: str
    score: float
    feed_name: str | None = None
    author: str | None = None
    published_at: str | None = None


class SearchResponse(BaseModel):
    results: list[WebSearchItem | NewsSearchItem]
    count: int


class SearchService:
    def _normalize_text(self, text: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()

    def _extract_query_words(self, text: str) -> list[str]:
        words: list[str] = []
        seen: set[str] = set()
        for term in TOKEN_RE.findall(text.lower()):
            if term in STOPWORDS or term in seen:
                continue
            words.append(term)
            seen.add(term)
        return words

    def _count_hits(self, text: str, query_words: list[str]) -> int:
        if not query_words:
            return 0
        word_set = set(TOKEN_RE.findall((text or "").lower()))
        return sum(1 for word in query_words if word in word_set)

    def _compact_word_hits(self, compact_text: str, query_words: list[str]) -> int:
        if not query_words or not compact_text:
            return 0
        return sum(1 for word in query_words if word in compact_text)

    def _intent_score(
        self,
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
        score = math.log1p(max(float(token_score), 0.0)) * 12.0
        if total_terms:
            coverage = matched_terms / total_terms
            score += coverage * 25.0
            if matched_terms == total_terms:
                score += 40.0

        normalized_title = self._normalize_text(title)
        normalized_description = self._normalize_text(description)
        normalized_url = self._normalize_text(url)
        compact_url = re.sub(r"[^a-z0-9]+", "", (url or "").lower())

        if query_phrase and query_phrase in normalized_title:
            score += 140.0
        if query_phrase and query_phrase in normalized_url:
            score += 70.0
        if query_phrase and query_phrase in normalized_description:
            score += 25.0
        if query_compact and query_compact in compact_url:
            score += 90.0

        title_hits = self._count_hits(title, query_words)
        url_hits = self._count_hits(url, query_words)
        compact_url_hits = self._compact_word_hits(compact_url, query_words)
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

    def _search_context(self, q: str, limit: int, offset: int) -> dict[str, Any] | None:
        query_terms = list(tokenize(q).keys())
        if not query_terms:
            return None

        query_words = self._extract_query_words(q)
        return {
            "query_terms": query_terms,
            "query_phrase": self._normalize_text(q),
            "query_compact": "".join(query_words),
            "query_words": query_words,
            "total_terms": len(query_terms),
            "candidate_limit": min(
                MAX_CANDIDATES,
                max(offset + limit + CANDIDATE_BUFFER, limit * 10),
            ),
        }

    def _rank_web_rows(self, rows: list[tuple[Any, ...]], *, context: dict[str, Any]) -> list[WebSearchItem]:
        ranked_results: list[WebSearchItem] = []
        for row in rows:
            title = row[0] or ""
            description = row[1] or ""
            url = row[2]
            score = self._intent_score(
                token_score=float(row[3] or 0.0),
                matched_terms=int(row[4] or 0),
                total_terms=context["total_terms"],
                query_phrase=context["query_phrase"],
                query_compact=context["query_compact"],
                query_words=context["query_words"],
                title=title,
                description=description,
                url=url,
            )
            ranked_results.append(WebSearchItem(title=title, description=description, url=url, score=score))

        ranked_results.sort(key=lambda item: (-item.score, item.url))
        return ranked_results

    def _rank_news_rows(self, rows: list[tuple[Any, ...]], *, context: dict[str, Any]) -> list[NewsSearchItem]:
        news_results: list[NewsSearchItem] = []
        for row in rows:
            title = row[0] or ""
            description = row[1] or ""
            url = row[2]
            score = self._intent_score(
                token_score=float(row[6] or 0.0),
                matched_terms=int(row[7] or 0),
                total_terms=context["total_terms"],
                query_phrase=context["query_phrase"],
                query_compact=context["query_compact"],
                query_words=context["query_words"],
                title=title,
                description=description,
                url=url,
            ) + 8.0
            news_results.append(
                NewsSearchItem(
                    title=title,
                    description=description,
                    url=url,
                    score=score,
                    feed_name=row[3],
                    author=row[4],
                    published_at=row[5].isoformat() if row[5] else None,
                )
            )
        return news_results

    def perform_web_search(self, *, q: str, limit: int = 20, offset: int = 0) -> SearchResponse:
        context = self._search_context(q, limit, offset)
        if not context:
            return SearchResponse(results=[], count=0)

        with get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(SEARCH_SQL, (context["query_terms"], context["candidate_limit"]))
                    rows = cur.fetchall()
            except CharacterNotInRepertoire:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("SET client_encoding TO SQL_ASCII")
                    cur.execute(FALLBACK_SEARCH_SQL, (context["query_terms"], context["candidate_limit"]))
                    rows = cur.fetchall()
                fallback_results: list[WebSearchItem] = []
                for row in rows:
                    score = math.log1p(max(float(row[0] or 0.0), 0.0)) * 12.0
                    matched_terms = int(row[1] or 0)
                    if context["total_terms"]:
                        score += (matched_terms / context["total_terms"]) * 25.0
                        if matched_terms == context["total_terms"]:
                            score += 40.0
                    fallback_results.append(WebSearchItem(title="", description="", url="", score=score))
                fallback_results.sort(key=lambda item: item.score, reverse=True)
                page = fallback_results[offset : offset + limit]
                return SearchResponse(results=page, count=max(len(fallback_results), offset + len(page)))

        ranked_results = self._rank_web_rows(rows, context=context)
        page = ranked_results[offset : offset + limit]
        return SearchResponse(results=page, count=max(len(ranked_results), offset + len(page)))

    def perform_news_search(self, *, q: str, limit: int = 20, offset: int = 0) -> SearchResponse:
        context = self._search_context(q, limit, offset)
        if not context:
            return SearchResponse(results=[], count=0)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(NEWS_SEARCH_SQL, (context["query_terms"], context["candidate_limit"]))
                rows = cur.fetchall()

        ranked = self._rank_news_rows(rows, context=context)
        page = ranked[offset : offset + limit]
        return SearchResponse(results=page, count=max(len(ranked), offset + len(page)))


search_service = SearchService()


def perform_web_search(*, q: str, limit: int = 20, offset: int = 0) -> SearchResponse:
    return search_service.perform_web_search(q=q, limit=limit, offset=offset)


def perform_news_search(*, q: str, limit: int = 20, offset: int = 0) -> SearchResponse:
    return search_service.perform_news_search(q=q, limit=limit, offset=offset)
