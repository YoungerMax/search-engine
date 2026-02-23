import asyncio

from app.common.db import get_conn_async


async def run() -> None:
    async with get_conn_async() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT AVG(word_count)::float FROM documents WHERE status='done'")
            avg_doc_len = (await cur.fetchone())[0] or 0.0

            await cur.execute("SELECT COUNT(*) FROM documents WHERE status='done'")
            doc_total = (await cur.fetchone())[0] or 1

            await cur.execute("TRUNCATE term_statistics")
            await cur.execute(
                """
                INSERT INTO term_statistics(term, doc_frequency, idf, avg_doc_len)
                SELECT t.term,
                       COUNT(DISTINCT t.doc_id) AS df,
                       LN((%s - COUNT(DISTINCT t.doc_id) + 0.5) / (COUNT(DISTINCT t.doc_id) + 0.5) + 1),
                       %s
                FROM tokens t
                GROUP BY t.term
                """,
                (doc_total, avg_doc_len),
            )


if __name__ == "__main__":
    asyncio.run(run())
