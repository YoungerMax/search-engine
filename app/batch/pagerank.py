import asyncio
from collections import defaultdict

from app.common.db import get_conn

DAMPING = 0.85
ITERATIONS = 20


async def run() -> None:
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id FROM documents WHERE status='done'")
            nodes = [r[0] for r in await cur.fetchall()]
            n = len(nodes)
            if n == 0:
                return

            idx = {node: i for i, node in enumerate(nodes)}
            outgoing = defaultdict(list)
            inlinks = defaultdict(int)

            await cur.execute("SELECT source_doc_id, target_doc_id FROM links_resolved")
            for s, t in await cur.fetchall():
                if s in idx and t in idx:
                    outgoing[s].append(t)
                    inlinks[t] += 1

            pr = {node: 1.0 / n for node in nodes}
            for _ in range(ITERATIONS):
                new_pr = {node: (1 - DAMPING) / n for node in nodes}
                for node in nodes:
                    targets = outgoing[node]
                    if targets:
                        share = DAMPING * pr[node] / len(targets)
                        for t in targets:
                            new_pr[t] += share
                pr = new_pr

            await cur.execute("""
                CREATE TEMP TABLE tmp_document_authority (
                  doc_id BIGINT PRIMARY KEY,
                  pagerank DOUBLE PRECISION NOT NULL,
                  inlink_count INT NOT NULL
                ) ON COMMIT DROP
            """)
            async with cur.copy("COPY tmp_document_authority(doc_id, pagerank, inlink_count) FROM STDIN") as copy:
                for node in nodes:
                    await copy.write_row((node, pr[node], inlinks[node]))

            await cur.execute("""
                INSERT INTO document_authority(doc_id, pagerank, inlink_count)
                SELECT doc_id, pagerank, inlink_count
                FROM tmp_document_authority
                ON CONFLICT (doc_id) DO UPDATE
                SET pagerank = EXCLUDED.pagerank,
                    inlink_count = EXCLUDED.inlink_count
            """)


if __name__ == "__main__":
    asyncio.run(run())
