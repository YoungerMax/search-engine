from dataclasses import dataclass
from datetime import datetime, timezone
import logging

from psycopg.rows import dict_row

from app.common.db import get_conn
from app.crawler.normalization import normalize_url, registrable_domain

logger = logging.getLogger(__name__)


@dataclass
class QueueItem:
    url: str
    domain: str


class QueueManager:
    async def enqueue_url(self, raw_url: str) -> None:
        url = normalize_url(raw_url)
        domain = registrable_domain(url)
        async with get_conn() as conn:
            cur = await conn.execute(
                """
                INSERT INTO crawl_queue(url, status, domain, attempt_count)
                VALUES (%s, 'queued', %s, 0)
                ON CONFLICT (url) DO NOTHING
                """,
                (url, domain),
            )
            logger.info("enqueue url=%s inserted=%s", url, cur.rowcount > 0)

    async def dequeue_many(self, limit: int) -> list[QueueItem]:
        async with get_conn() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    WITH next_urls AS (
                      SELECT q.url, q.domain
                      FROM crawl_queue q
                      WHERE q.status = 'queued'
                      ORDER BY random()
                      LIMIT %s
                      FOR UPDATE OF q SKIP LOCKED
                    )
                    UPDATE crawl_queue q
                    SET status = 'in_progress',
                        last_attempt = now(),
                        attempt_count = attempt_count + 1
                    FROM next_urls
                    WHERE q.url = next_urls.url
                    RETURNING q.url, q.domain
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
                logger.info("dequeue requested=%s returned=%s", limit, len(rows))
                return [QueueItem(url=r["url"], domain=r["domain"]) for r in rows]

    async def mark_status(self, url: str, status: str) -> None:
        async with get_conn() as conn:
            cur = await conn.execute(
                "UPDATE crawl_queue SET status=%s, last_attempt=%s WHERE url=%s",
                (status, datetime.now(timezone.utc), url),
            )
            logger.info("mark_status url=%s status=%s updated=%s", url, status, cur.rowcount)
