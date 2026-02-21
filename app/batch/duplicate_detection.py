from simhash import Simhash

from app.common.db import get_conn


def _to_pg_bigint(value: int) -> int:
    """Map unsigned 64-bit value to signed 64-bit (PostgreSQL BIGINT)."""
    if value >= (1 << 63):
        return value - (1 << 64)
    return value


def run() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, content FROM documents WHERE status='done'")
            rows = cur.fetchall()
            for doc_id, content in rows:
                fp = _to_pg_bigint(Simhash(content.split()).value)
                cur.execute(
                    """
                    INSERT INTO document_fingerprints(doc_id, fingerprint)
                    VALUES (%s,%s)
                    ON CONFLICT(doc_id) DO UPDATE SET fingerprint=EXCLUDED.fingerprint
                    """,
                    (doc_id, fp),
                )


if __name__ == "__main__":
    run()
