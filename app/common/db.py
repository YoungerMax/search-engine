from contextlib import contextmanager
from typing import Iterator
import os

import psycopg
from dotenv import load_dotenv

load_dotenv()


def _conninfo() -> str:
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    # service name in compose/swarm network
    return f"postgresql://{user}:{password}@postgres:5432/{db}"


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(_conninfo())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
