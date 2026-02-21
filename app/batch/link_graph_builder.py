from app.common.db import get_conn


def run() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE links_resolved")
            cur.execute(
                """
                INSERT INTO links_resolved(source_doc_id, target_doc_id)
                SELECT DISTINCT lo.source_doc_id, d.id
                FROM links_outgoing lo
                JOIN documents d ON d.url = lo.target_url
                ON CONFLICT (source_doc_id, target_doc_id) DO NOTHING
                """
            )


if __name__ == "__main__":
    run()
