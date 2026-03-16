"""TimescaleDB connection pool management."""

import logging
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool

logger = logging.getLogger("database")

_pool: pool.ThreadedConnectionPool | None = None


def init_pool(db_cfg: dict) -> pool.ThreadedConnectionPool:
    """Initialize a threaded connection pool.

    Args:
        db_cfg: TimescaleDB configuration section.

    Returns:
        ThreadedConnectionPool instance.
    """
    global _pool
    if _pool is not None:
        return _pool

    _pool = pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=db_cfg.get("pool_size", 5) + db_cfg.get("max_overflow", 10),
        host=db_cfg["host"],
        port=db_cfg["port"],
        dbname=db_cfg["database"],
        user=db_cfg["user"],
        password=str(db_cfg["password"]),
    )
    logger.info("Connection pool initialized (host=%s, db=%s)",
                db_cfg["host"], db_cfg["database"])
    return _pool


@contextmanager
def get_connection(db_cfg: dict):
    """Acquire a connection from the pool with automatic release.

    Usage:
        with get_connection(db_cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(...)
    """
    p = init_pool(db_cfg)
    conn = p.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


def close_pool() -> None:
    """Close all connections in the pool."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        logger.info("Connection pool closed")
