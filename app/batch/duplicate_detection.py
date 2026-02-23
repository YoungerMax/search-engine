import asyncio
import os

from simhash import Simhash

from app.common.db import get_conn_async

BATCH_SIZE = 2000


def _to_pg_bigint(value: int) -> int:
    if value >= (1 << 63):
        return value - (1 << 64)
    return value


async def _flush_rows(cur, rows: list[tuple[int, int]]) -> None:
    if not rows:
        return

    await cur.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS tmp_document_fingerprints (
          doc_id BIGINT PRIMARY KEY,
          fingerprint BIGINT NOT NULL
        ) ON COMMIT DROP
        """
    )
    async with cur.copy("COPY tmp_document_fingerprints(doc_id, fingerprint) FROM STDIN") as copy:
        for row in rows:
            await copy.write_row(row)

    await cur.execute(
        """
        INSERT INTO document_fingerprints(doc_id, fingerprint)
        SELECT doc_id, fingerprint
        FROM tmp_document_fingerprints
        ON CONFLICT(doc_id) DO UPDATE
        SET fingerprint = EXCLUDED.fingerprint
        """
    )
    await cur.execute("TRUNCATE tmp_document_fingerprints")


async def run() -> None:
    total_nodes = max(1, int(os.environ.get("BATCH_TOTAL_NODES", "1")))
    node_index = int(os.environ.get("BATCH_NODE_INDEX", "0"))

    async with get_conn_async() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, content
                FROM documents
                WHERE status='done'
                  AND mod(id, %s) = %s
                """,
                (total_nodes, node_index),
            )
            source_rows = await cur.fetchall()
            rows: list[tuple[int, int]] = []
            for doc_id, content in source_rows:
                fp = _to_pg_bigint(Simhash((content or "").split()).value)
                rows.append((doc_id, fp))
                if len(rows) >= BATCH_SIZE:
                    await _flush_rows(cur, rows)
                    rows.clear()

            await _flush_rows(cur, rows)


if __name__ == "__main__":
    asyncio.run(run())
