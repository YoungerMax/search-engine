from contextlib import asynccontextmanager
from typing import AsyncIterator
import os

import psycopg
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
async def get_conn() -> AsyncIterator[psycopg.AsyncConnection]:
    conn = await psycopg.AsyncConnection.connect(_conninfo())
    try:
        yield conn
        await conn.commit()
    except Exception:
        await conn.rollback()
        raise
    finally:
        await conn.close()
