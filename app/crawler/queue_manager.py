from dataclasses import dataclass
from datetime import datetime, timezone

from psycopg.rows import dict_row

from app.common.db import get_conn
from app.crawler.normalization import normalize_url


@dataclass
class QueueItem:
    url: str
    domain: str


class QueueManager:
    def enqueue_url(self, raw_url: str) -> None:
        url = normalize_url(raw_url)
        domain = url.split("/")[2] if "/" in url else url
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO crawl_queue(url, status, domain, attempt_count)
                VALUES (%s, 'queued', %s, 0)
                ON CONFLICT (url) DO NOTHING
                """,
                (url, domain),
            )

    def dequeue_many(self, limit: int) -> list[QueueItem]:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    WITH next_urls AS (
                      SELECT url, domain
                      FROM crawl_queue
                      WHERE status = 'queued'
                      ORDER BY last_attempt NULLS FIRST, attempt_count ASC
                      LIMIT %s
                      FOR UPDATE SKIP LOCKED
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
                rows = cur.fetchall()
                return [QueueItem(url=r["url"], domain=r["domain"]) for r in rows]

    def mark_status(self, url: str, status: str) -> None:
        with get_conn() as conn:
            conn.execute(
                "UPDATE crawl_queue SET status=%s, last_attempt=%s WHERE url=%s",
                (status, datetime.now(timezone.utc), url),
            )
