from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
import time
from urllib.parse import urljoin, urlsplit

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from readability import Document

from app.common.config import settings
from app.common.db import get_conn
from app.crawler.normalization import normalize_url, registrable_domain
from app.crawler.queue_manager import QueueItem, QueueManager
from app.crawler.tokenizer import tokenize

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DomainRateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        self._min_interval_s = 1.0 / requests_per_second
        self._next_allowed_at: dict[str, float] = {}

    async def wait(self, domain: str) -> None:
        now = time.monotonic()
        next_allowed = self._next_allowed_at.get(domain, 0.0)
        # Reserve our slot immediately, before sleeping.
        # Each concurrent caller advances the window forward, so they queue up.
        my_slot = max(now, next_allowed)
        self._next_allowed_at[domain] = my_slot + self._min_interval_s
        wait_s = my_slot - now
        if wait_s > 0:
            await asyncio.sleep(wait_s)


domain_rate_limiter = DomainRateLimiter(requests_per_second=0.5)


@dataclass
class ParsedPage:
    title: str
    description: str
    content: str
    links: list[str]
    feed_links: list[str]
    published_at: datetime | None
    updated_at: datetime | None


def parse_html(url: str, html: str) -> ParsedPage:
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""
    desc_tag = soup.find("meta", attrs={"name": "description"})
    description = (desc_tag.get("content") or "").strip() if desc_tag else ""

    doc = Document(html)
    readable_soup = BeautifulSoup(doc.summary(), "html.parser")
    content = readable_soup.get_text(" ", strip=True)

    seen_links: set[str] = set()
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        try:
            normalized = normalize_url(urljoin(url, a["href"]))
            if normalized not in seen_links:
                seen_links.add(normalized)
                links.append(normalized)
        except Exception:
            continue

    pub = _extract_ts(soup, "article:published_time")
    upd = _extract_ts(soup, "article:modified_time")
    return ParsedPage(
        title=title,
        description=description,
        content=content,
        links=links,
        feed_links=_extract_feed_links(url, soup),
        published_at=pub,
        updated_at=upd,
    )


def _extract_feed_links(base_url: str, soup: BeautifulSoup) -> list[str]:
    discovered: list[str] = []
    seen: set[str] = set()

    def _add_candidate(raw_value: str) -> None:
        value = raw_value.strip()
        if not any(marker in value.lower() for marker in ("rss", "atom", "feed", ".xml")):
            return
        try:
            normalized = normalize_url(urljoin(base_url, value))
        except Exception:
            return
        if normalized not in seen:
            seen.add(normalized)
            discovered.append(normalized)

    for link in soup.find_all("link"):
        href = (link.get("href") or "").strip()
        if not href:
            continue
        rel = link.get("rel", [])
        rel_text = " ".join(rel) if isinstance(rel, list) else str(rel)
        feed_type = (link.get("type") or "").lower()
        if any(marker in feed_type for marker in ("rss", "atom")):
            if "alternate" in rel_text.lower() or not rel_text:
                _add_candidate(href)
        elif any(marker in rel_text.lower() for marker in ("alternate", "feed", "rss", "atom")):
            _add_candidate(href)

    for meta in soup.find_all("meta"):
        meta_name = (meta.get("name") or meta.get("property") or "").lower()
        if any(marker in meta_name for marker in ("rss", "atom", "feed")):
            for attr in ("content", "value", "href"):
                if val := meta.get(attr):
                    _add_candidate(str(val))

    return discovered


def _extract_ts(soup: BeautifulSoup, prop: str) -> datetime | None:
    node = soup.find("meta", attrs={"property": prop})
    if not node or not node.get("content"):
        return None
    try:
        dt = date_parser.parse(node["content"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt if dt <= datetime.now(timezone.utc) else None
    except Exception:
        return None


def _is_feed_content_type(content_type: str) -> bool:
    return any(marker in content_type.lower() for marker in ("rss", "atom", "xml"))


def _looks_like_feed(text: str) -> bool:
    """Check the first 512 bytes of a response body for RSS/Atom markers."""
    sniff = text[:512].lstrip()
    return any(marker in sniff for marker in ("<rss", "<feed", "<atom"))


def compute_quality(content: str, outbound_link_count: int) -> float:
    wc = len(content.split())
    if wc == 0:
        return 0.0
    density = min(1.0, wc / 300)
    link_penalty = min(0.4, outbound_link_count / wc)
    return max(0.0, density - link_penalty)


def compute_freshness(updated_at: datetime | None, published_at: datetime | None) -> float:
    ts = updated_at or published_at
    if not ts:
        return 0.1
    days = (datetime.now(timezone.utc) - ts).days
    return max(0.0, 1.0 - min(365, days) / 365)


def _register_feed_url(feed_url: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO news_feeds(feed_url, home_url, discovered_by_url)
                VALUES (%s, %s, %s)
                ON CONFLICT(feed_url) DO UPDATE SET
                  home_url = COALESCE(news_feeds.home_url, EXCLUDED.home_url),
                  discovered_by_url = COALESCE(news_feeds.discovered_by_url, EXCLUDED.discovered_by_url)
                """,
                (feed_url, feed_url, feed_url),
            )


def _backfill_news_article_content(url: str, content: str) -> None:
    if not content or len(content.strip()) < 120:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE news_articles
                SET content = %s, updated_at = now()
                WHERE url = %s AND COALESCE(content, '') = ''
                """,
                (content, url),
            )


def _persist(url: str, parsed: ParsedPage, quality: float, freshness: float) -> None:
    title_tokens = tokenize(parsed.title)
    desc_tokens = tokenize(parsed.description)
    body_tokens = tokenize(parsed.content)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO documents(
                    url, canonical_url, title, description, content,
                    published_at, updated_at, word_count, quality_score, freshness_score, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'done')
                ON CONFLICT(url) DO UPDATE SET
                  title = EXCLUDED.title,
                  description = EXCLUDED.description,
                  content = EXCLUDED.content,
                  published_at = EXCLUDED.published_at,
                  updated_at = EXCLUDED.updated_at,
                  word_count = EXCLUDED.word_count,
                  quality_score = EXCLUDED.quality_score,
                  freshness_score = EXCLUDED.freshness_score,
                  status = 'done'
                RETURNING id
                """,
                (
                    url, url, parsed.title, parsed.description, parsed.content,
                    parsed.published_at, parsed.updated_at,
                    len(parsed.content.split()), quality, freshness,
                ),
            )
            doc_id = cur.fetchone()[0]

            cur.execute("DELETE FROM tokens WHERE doc_id = %s", (doc_id,))
            token_rows = [
                (doc_id, term, field, freq, [])
                for field, counter in ((1, title_tokens), (2, desc_tokens), (4, body_tokens))
                for term, freq in counter.items()
            ]
            if token_rows:
                cur.executemany(
                    "INSERT INTO tokens(doc_id, term, field, frequency, positions) VALUES (%s, %s, %s, %s, %s)",
                    token_rows,
                )


            if parsed.feed_links:
                cur.executemany(
                    """
                    INSERT INTO news_feeds(feed_url, home_url, discovered_by_url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT(feed_url) DO UPDATE SET
                      home_url = COALESCE(news_feeds.home_url, EXCLUDED.home_url),
                      discovered_by_url = COALESCE(news_feeds.discovered_by_url, EXCLUDED.discovered_by_url)
                    """,
                    ((feed, url, url) for feed in set(parsed.feed_links)),
                )

            cur.execute("DELETE FROM links_outgoing WHERE source_doc_id = %s", (doc_id,))
            if parsed.links:
                cur.executemany(
                    "INSERT INTO links_outgoing(source_doc_id, target_url) VALUES (%s, %s)",
                    ((doc_id, link) for link in parsed.links),
                )
                cur.executemany(
                    """
                    INSERT INTO crawl_queue(url, status, domain, attempt_count)
                    VALUES (%s, 'queued', %s, 0)
                    ON CONFLICT(url) DO NOTHING
                    """,
                    ((link, registrable_domain(link)) for link in parsed.links),
                )


async def process_item(item: QueueItem, client: httpx.AsyncClient) -> None:
    domain = item.domain or registrable_domain(item.url) or urlsplit(item.url).netloc
    await domain_rate_limiter.wait(domain)

    logger.info("fetching url=%s domain=%s", item.url, domain)
    queue_manager = QueueManager()
    try:
        res = await client.get(
            item.url,
            headers={"Accept": "text/html", "User-Agent": settings.user_agent},
            follow_redirects=True,
        )
        logger.info("fetched url=%s status_code=%s", item.url, res.status_code)

        if res.status_code >= 400:
            logger.warning("non-success status for url=%s status_code=%s", item.url, res.status_code)
            await asyncio.to_thread(queue_manager.mark_status, item.url, "non_success_status_error")
            return

        content_type = res.headers.get("content-type", "")

        # application/xml could be RSS or Atom — sniff the body to confirm
        if _is_feed_content_type(content_type):
            if "xml" in content_type.lower() and not any(m in content_type.lower() for m in ("rss", "atom")):
                if not _looks_like_feed(res.text):
                    logger.warning("xml but not a feed url=%s", item.url)
                    await asyncio.to_thread(queue_manager.mark_status, item.url, "processing_error")
                    return
            await asyncio.to_thread(_register_feed_url, item.url)
            logger.info("registered feed url=%s content_type=%s", item.url, content_type)
            await asyncio.to_thread(queue_manager.mark_status, item.url, "done")
            return

        if "text/html" not in content_type.lower():
            logger.warning("non-html response for url=%s content_type=%s", item.url, content_type)
            await asyncio.to_thread(queue_manager.mark_status, item.url, "processing_error")
            return

        parsed = await asyncio.to_thread(parse_html, item.url, res.text)
        await asyncio.to_thread(_backfill_news_article_content, item.url, parsed.content)

        if not (parsed.title and parsed.description and parsed.content and len(parsed.content) >= 120):
            logger.warning("validation failed for url=%s", item.url)
            await asyncio.to_thread(queue_manager.mark_status, item.url, "validation_error")
            return

        quality = compute_quality(parsed.content, len(parsed.links))
        freshness = compute_freshness(parsed.updated_at, parsed.published_at)
        await asyncio.to_thread(_persist, item.url, parsed, quality, freshness)
        logger.info(
            "finished url=%s word_count=%s links=%s quality=%.3f freshness=%.3f",
            item.url, len(parsed.content.split()), len(parsed.links), quality, freshness,
        )
        await asyncio.to_thread(queue_manager.mark_status, item.url, "done")

    except (httpx.TimeoutException, httpx.RequestError):
        logger.exception("request timeout/error for %s", item.url)
        await asyncio.to_thread(queue_manager.mark_status, item.url, "processing_error")
    except Exception:
        logger.exception("processing error for %s", item.url)
        await asyncio.to_thread(queue_manager.mark_status, item.url, "processing_error")


async def run_worker() -> None:
    qm = QueueManager()
    concurrency = max(1, settings.crawler_concurrency)
    dequeue_size = max(settings.queue_batch_size, concurrency * 4)
    logger.info(
        "crawler worker started batch_size=%s concurrency=%s dequeue_size=%s",
        settings.queue_batch_size, concurrency, dequeue_size,
    )

    timeout = httpx.Timeout(settings.request_timeout_s)
    limits = httpx.Limits(
        max_connections=max(32, concurrency * 8),
        max_keepalive_connections=max(16, concurrency * 4),
        keepalive_expiry=30.0,
    )

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        pending: list[QueueItem] = []
        in_flight: set[asyncio.Task[None]] = set()

        while True:
            if len(pending) < dequeue_size:
                items = await asyncio.to_thread(qm.dequeue_many, dequeue_size - len(pending))

                if items:
                    pending.extend(items)
                    logger.info("dequeued %s item(s) pending=%s", len(items), len(pending))

            while len(in_flight) < concurrency and pending:
                item = pending.pop(0)
                in_flight.add(asyncio.create_task(process_item(item, client)))

            if in_flight:
                done, in_flight = await asyncio.wait(in_flight, timeout=0.2, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task.exception() is not None:
                        logger.exception("worker task failed", exc_info=task.exception())
                continue

            if not pending:
                logger.info("queue empty, sleeping for 0.5s")
                await asyncio.sleep(0.5)


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
