from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator
import os

import psycopg
from psycopg import Connection
from dotenv import load_dotenv

load_dotenv()


def _conninfo() -> str:
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@asynccontextmanager
async def get_conn_async() -> AsyncIterator[psycopg.AsyncConnection]:
    conn = await psycopg.AsyncConnection.connect(_conninfo())
    try:
        yield conn
        await conn.commit()
    except Exception:
        await conn.rollback()
        raise
    finally:
        await conn.close()


@contextmanager
def get_conn() -> Iterator[Connection]:
    conn = psycopg.Connection.connect(_conninfo())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()