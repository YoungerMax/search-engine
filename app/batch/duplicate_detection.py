from simhash import Simhash

from app.common.db import get_conn


def run() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, content FROM documents WHERE status='done'")
            rows = cur.fetchall()
            for doc_id, content in rows:
                fp = Simhash(content.split()).value
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
