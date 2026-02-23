import os

from simhash import Simhash

from app.common.db import get_conn

BATCH_SIZE = 2000


def _to_pg_bigint(value: int) -> int:
    """Map unsigned 64-bit value to signed 64-bit (PostgreSQL BIGINT)."""
    if value >= (1 << 63):
        return value - (1 << 64)
    return value


def _flush_rows(cur, rows: list[tuple[int, int]]) -> None:
    if not rows:
        return

    cur.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS tmp_document_fingerprints (
          doc_id BIGINT PRIMARY KEY,
          fingerprint BIGINT NOT NULL
        ) ON COMMIT DROP
        """
    )
    with cur.copy("COPY tmp_document_fingerprints(doc_id, fingerprint) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)

    cur.execute(
        """
        INSERT INTO document_fingerprints(doc_id, fingerprint)
        SELECT doc_id, fingerprint
        FROM tmp_document_fingerprints
        ON CONFLICT(doc_id) DO UPDATE
        SET fingerprint = EXCLUDED.fingerprint
        """
    )
    cur.execute("TRUNCATE tmp_document_fingerprints")


def run() -> None:
    total_nodes = max(1, int(os.environ.get("BATCH_TOTAL_NODES", "1")))
    node_index = int(os.environ.get("BATCH_NODE_INDEX", "0"))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, content
                FROM documents
                WHERE status='done'
                  AND mod(id, %s) = %s
                """,
                (total_nodes, node_index),
            )
            rows: list[tuple[int, int]] = []
            for doc_id, content in cur:
                fp = _to_pg_bigint(Simhash((content or "").split()).value)
                rows.append((doc_id, fp))
                if len(rows) >= BATCH_SIZE:
                    _flush_rows(cur, rows)
                    rows.clear()

            _flush_rows(cur, rows)


if __name__ == "__main__":
    run()
