import asyncio

from app.common.db import get_conn


async def run() -> None:
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE links_resolved")
            await cur.execute(
                """
                INSERT INTO links_resolved(source_doc_id, target_doc_id)
                SELECT DISTINCT lo.source_doc_id, d.id
                FROM links_outgoing lo
                JOIN documents d ON d.url = lo.target_url
                ON CONFLICT (source_doc_id, target_doc_id) DO NOTHING
                """
            )


if __name__ == "__main__":
    asyncio.run(run())
