from __future__ import annotations

import math
import re
from typing import Any

from pydantic import BaseModel
from psycopg.errors import CharacterNotInRepertoire

from app.common.db import get_conn_async
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
       nf.feed_url,
       nf.home_url,
       nf.name,
       nf.link,
       nf.image,
       nf.discovered_by_url,
       nf.last_published,
       nf.last_fetched,
       nf.next_fetch_at,
       nf.publish_rate_per_hour,
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
GROUP BY na.title, na.description, na.url,
         nf.feed_url, nf.home_url, nf.name, nf.link, nf.image, nf.discovered_by_url,
         nf.last_published, nf.last_fetched, nf.next_fetch_at, nf.publish_rate_per_hour,
         na.author, na.published_at
ORDER BY token_score DESC, na.url ASC
LIMIT %s
"""

CANDIDATE_BUFFER = 200
MAX_CANDIDATES = 2000




class NewsFeed(BaseModel):
    feed_url: str | None = None
    home_url: str | None = None
    name: str | None = None
    link: str | None = None
    image: str | None = None
    discovered_by_url: str | None = None
    last_published: str | None = None
    last_fetched: str | None = None
    next_fetch_at: str | None = None
    publish_rate_per_hour: float | None = None


class WebSearchItem(BaseModel):
    title: str
    description: str
    url: str
    score: float


class NewsSearchItem(BaseModel):
    title: str
    description: str
    url: str
    score: float
    feed: NewsFeed | None = None
    author: str | None = None
    published_at: str | None = None


class WebSearchResponse(BaseModel):
    results: list[WebSearchItem]
    count: int


class NewsSearchResponse(BaseModel):
    results: list[NewsSearchItem]
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


    def _news_feed_from_row(self, row: tuple[Any, ...]) -> NewsFeed | None:
        if not row[3]:
            return None
        return NewsFeed(
            feed_url=row[3],
            home_url=row[4],
            name=row[5],
            link=row[6],
            image=row[7],
            discovered_by_url=row[8],
            last_published=row[9].isoformat() if row[9] else None,
            last_fetched=row[10].isoformat() if row[10] else None,
            next_fetch_at=row[11].isoformat() if row[11] else None,
            publish_rate_per_hour=float(row[12]) if row[12] is not None else None,
        )

    def _rank_news_rows(self, rows: list[tuple[Any, ...]], *, context: dict[str, Any]) -> list[NewsSearchItem]:
        news_results: list[NewsSearchItem] = []
        for row in rows:
            title = row[0] or ""
            description = row[1] or ""
            url = row[2]
            score = self._intent_score(
                token_score=float(row[15] or 0.0),
                matched_terms=int(row[16] or 0),
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
                    feed=self._news_feed_from_row(row),
                    author=row[13],
                    published_at=row[14].isoformat() if row[14] else None,
                )
            )
        return news_results

    async def perform_web_search(self, *, q: str, limit: int = 20, offset: int = 0) -> WebSearchResponse:
        context = self._search_context(q, limit, offset)
        if not context:
            return WebSearchResponse(results=[], count=0)

        async with get_conn_async() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(SEARCH_SQL, (context["query_terms"], context["candidate_limit"]))
                    rows = await cur.fetchall()
            except CharacterNotInRepertoire:
                await conn.rollback()
                async with conn.cursor() as cur:
                    await cur.execute("SET client_encoding TO SQL_ASCII")
                    await cur.execute(FALLBACK_SEARCH_SQL, (context["query_terms"], context["candidate_limit"]))
                    rows = await cur.fetchall()
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
                return WebSearchResponse(results=page, count=max(len(fallback_results), offset + len(page)))

        ranked_results = self._rank_web_rows(rows, context=context)
        page = ranked_results[offset : offset + limit]
        return WebSearchResponse(results=page, count=max(len(ranked_results), offset + len(page)))

    async def perform_news_search(self, *, q: str, limit: int = 20, offset: int = 0) -> NewsSearchResponse:
        context = self._search_context(q, limit, offset)
        if not context:
            return NewsSearchResponse(results=[], count=0)

        async with get_conn_async() as conn:
            async with conn.cursor() as cur:
                await cur.execute(NEWS_SEARCH_SQL, (context["query_terms"], context["candidate_limit"]))
                rows = await cur.fetchall()

        ranked = self._rank_news_rows(rows, context=context)
        page = ranked[offset : offset + limit]
        return NewsSearchResponse(results=page, count=max(len(ranked), offset + len(page)))


search_service = SearchService()


async def perform_web_search(*, q: str, limit: int = 20, offset: int = 0) -> WebSearchResponse:
    return await search_service.perform_web_search(q=q, limit=limit, offset=offset)


async def perform_news_search(*, q: str, limit: int = 20, offset: int = 0) -> NewsSearchResponse:
    return await search_service.perform_news_search(q=q, limit=limit, offset=offset)

SearchResponse = WebSearchResponse | NewsSearchResponse
