from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
import time
from urllib.parse import urljoin
from urllib.parse import urlsplit

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from readability import Document

from app.common.config import settings
from app.common.db import get_conn
from app.crawler.normalization import normalize_url
from app.crawler.queue_manager import QueueItem, QueueManager
from app.crawler.tokenizer import tokenize

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DomainRateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        self._min_interval_s = 1.0 / requests_per_second
        self._lock = Lock()
        self._next_allowed_at: dict[str, float] = {}

    def reserve_if_available(self, domain: str) -> bool:
        now = time.monotonic()
        with self._lock:
            next_allowed = self._next_allowed_at.get(domain, 0.0)
            if now < next_allowed:
                return False
            self._next_allowed_at[domain] = now + self._min_interval_s
            return True

    def seconds_until_available(self, domain: str) -> float:
        now = time.monotonic()
        with self._lock:
            next_allowed = self._next_allowed_at.get(domain, 0.0)
        return max(0.0, next_allowed - now)


domain_rate_limiter = DomainRateLimiter(requests_per_second=1.0)


@dataclass
class ParsedPage:
    title: str
    description: str
    content: str
    links: list[str]
    published_at: datetime | None
    updated_at: datetime | None


def parse_html(url: str, html: str) -> ParsedPage:
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""
    desc_tag = soup.find("meta", attrs={"name": "description"})
    description = (desc_tag.get("content") or "").strip() if desc_tag else ""

    doc = Document(html)
    readable_html = doc.summary()
    readable_soup = BeautifulSoup(readable_html, "html.parser")
    content = readable_soup.get_text(" ", strip=True)

    links: list[str] = []
    seen_links: set[str] = set()
    for a in soup.find_all("a", href=True):
        try:
            absolute = urljoin(url, a["href"])
            normalized = normalize_url(absolute)
            if normalized in seen_links:
                continue
            seen_links.add(normalized)
            links.append(normalized)
        except Exception:
            continue

    pub = _extract_ts(soup, "article:published_time")
    upd = _extract_ts(soup, "article:modified_time")
    return ParsedPage(title=title, description=description, content=content, links=links, published_at=pub, updated_at=upd)


def _extract_ts(soup: BeautifulSoup, prop: str) -> datetime | None:
    node = soup.find("meta", attrs={"property": prop})
    if not node or not node.get("content"):
        return None
    try:
        dt = date_parser.parse(node["content"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt > datetime.now(timezone.utc):
            return None
        return dt
    except Exception:
        return None


def compute_quality(content: str, outbound_link_count: int) -> float:
    wc = len(content.split())
    if wc == 0:
        return 0.0
    density = min(1.0, wc / 300)
    link_penalty = min(0.4, outbound_link_count / max(1, wc))
    return max(0.0, density - link_penalty)


def compute_freshness(updated_at: datetime | None, published_at: datetime | None) -> float:
    ts = updated_at or published_at
    if not ts:
        return 0.1
    days = (datetime.now(timezone.utc) - ts).days
    return max(0.0, 1.0 - min(365, days) / 365)


async def process_item(item: QueueItem, client: httpx.AsyncClient) -> None:
    logger.info("processing url=%s domain=%s", item.url, item.domain)
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
        if "text/html" not in content_type.lower():
            logger.warning("non-html response for url=%s content_type=%s", item.url, content_type)
            await asyncio.to_thread(queue_manager.mark_status, item.url, "processing_error")
            return

        parsed = await asyncio.to_thread(parse_html, item.url, res.text)
        if not (parsed.title and parsed.description and parsed.content and len(parsed.content) >= 120):
            logger.warning("validation failed for url=%s", item.url)
            await asyncio.to_thread(queue_manager.mark_status, item.url, "validation_error")
            return

        quality = compute_quality(parsed.content, len(parsed.links))
        freshness = compute_freshness(parsed.updated_at, parsed.published_at)
        await asyncio.to_thread(_persist, item.url, parsed, quality, freshness)
        logger.info(
            "processed url=%s word_count=%s links=%s quality=%.3f freshness=%.3f",
            item.url,
            len(parsed.content.split()),
            len(parsed.links),
            quality,
            freshness,
        )
        await asyncio.to_thread(queue_manager.mark_status, item.url, "done")
    except (httpx.TimeoutException, httpx.RequestError):
        logger.exception("request timeout/error for %s", item.url)
        await asyncio.to_thread(queue_manager.mark_status, item.url, "processing_error")
    except Exception:
        logger.exception("processing error for %s", item.url)
        await asyncio.to_thread(queue_manager.mark_status, item.url, "processing_error")


def _domain_for_item(item: QueueItem) -> str:
    return item.domain or urlsplit(item.url).netloc


def _pop_next_ready_item(pending: list[QueueItem]) -> QueueItem | None:
    for idx, item in enumerate(pending):
        if domain_rate_limiter.reserve_if_available(_domain_for_item(item)):
            return pending.pop(idx)
    return None


def _min_domain_wait_s(pending: list[QueueItem]) -> float:
    if not pending:
        return 0.0
    return min(domain_rate_limiter.seconds_until_available(_domain_for_item(item)) for item in pending)


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
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'done')
                ON CONFLICT(url) DO UPDATE SET
                  title=EXCLUDED.title,
                  description=EXCLUDED.description,
                  content=EXCLUDED.content,
                  published_at=EXCLUDED.published_at,
                  updated_at=EXCLUDED.updated_at,
                  word_count=EXCLUDED.word_count,
                  quality_score=EXCLUDED.quality_score,
                  freshness_score=EXCLUDED.freshness_score,
                  status='done'
                RETURNING id
                """,
                (
                    url,
                    url,
                    parsed.title,
                    parsed.description,
                    parsed.content,
                    parsed.published_at,
                    parsed.updated_at,
                    len(parsed.content.split()),
                    quality,
                    freshness,
                ),
            )
            doc_id = cur.fetchone()[0]

            cur.execute("DELETE FROM tokens WHERE doc_id=%s", (doc_id,))
            token_rows: list[tuple[int, str, int, list[int]]] = []
            for field, counter in ((1, title_tokens), (2, desc_tokens), (4, body_tokens)):
                token_rows.extend((field, term, freq, []) for term, freq in counter.items())
            if token_rows:
                cur.executemany(
                    "INSERT INTO tokens(doc_id, term, field, frequency, positions) VALUES (%s,%s,%s,%s,%s)",
                    ((doc_id, term, field, freq, positions) for field, term, freq, positions in token_rows),
                )

            cur.execute("DELETE FROM links_outgoing WHERE source_doc_id=%s", (doc_id,))
            if parsed.links:
                cur.executemany(
                    "INSERT INTO links_outgoing(source_doc_id, target_url) VALUES (%s,%s)",
                    ((doc_id, link) for link in parsed.links),
                )
                cur.executemany(
                    """
                    INSERT INTO crawl_queue(url, status, domain, attempt_count)
                    VALUES (%s, 'queued', split_part(%s,'/',3), 0)
                    ON CONFLICT(url) DO NOTHING
                    """,
                    ((link, link) for link in parsed.links),
                )


async def run_worker() -> None:
    qm = QueueManager()
    concurrency = max(1, settings.crawler_concurrency)
    dequeue_size = max(settings.queue_batch_size, concurrency * 4)
    logger.info(
        "crawler worker started batch_size=%s concurrency=%s dequeue_size=%s",
        settings.queue_batch_size,
        concurrency,
        dequeue_size,
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

            submitted = 0
            while len(in_flight) < concurrency:
                next_item = _pop_next_ready_item(pending)
                if next_item is None:
                    break
                in_flight.add(asyncio.create_task(process_item(next_item, client)))
                submitted += 1

            if submitted:
                logger.info("submitted=%s in_flight=%s pending=%s", submitted, len(in_flight), len(pending))
                continue

            if in_flight:
                sleep_s = min(0.2, _min_domain_wait_s(pending)) if pending else 0.2
                done, not_done = await asyncio.wait(
                    in_flight,
                    timeout=sleep_s,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    if task.exception() is not None:
                        logger.exception("worker task failed", exc_info=task.exception())
                in_flight = not_done
                continue

            if pending:
                sleep_s = max(0.01, min(0.2, _min_domain_wait_s(pending)))
                await asyncio.sleep(sleep_s)
                continue

            logger.info("queue empty, sleeping for 0.5s")
            await asyncio.sleep(0.5)


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
