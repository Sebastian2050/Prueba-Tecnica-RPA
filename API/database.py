"""
database.py — Pool de conexiones a PostgreSQL.

Usa psycopg2 con un SimpleConnectionPool para reutilizar conexiones
entre requests sin abrir una nueva por cada llamada a la API.

El pool se inicializa una sola vez al arrancar la aplicación (lifespan)
y se cierra limpiamente al apagar.  Cada endpoint obtiene una conexión
del pool, la usa y la devuelve (patron get/put).

Parámetros del pool:
    minconn = 1  — conexión siempre disponible, evita latencia en el
                   primer request después de un período de inactividad.
    maxconn = 10 — máximo de conexiones simultáneas; suficiente para
                   cargas de trabajo analíticas donde las queries son
                   lentas pero poco concurrentes.
"""

import logging
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from config import DB_CONFIG

log = logging.getLogger(__name__)

# Pool global — se asigna en startup(), se cierra en shutdown().
_pool: pool.SimpleConnectionPool | None = None


def startup() -> None:
    """
    Inicializa el pool de conexiones.
    Debe llamarse una vez al arrancar la app (evento lifespan de FastAPI).
    """
    global _pool
    _pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, **DB_CONFIG)
    log.info(
        f"[DB] Pool iniciado → "
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )


def shutdown() -> None:
    """Cierra todas las conexiones del pool al apagar la app."""
    if _pool:
        _pool.closeall()
        log.info("[DB] Pool cerrado")


@contextmanager
def get_conn():
    """
    Context manager que entrega una conexión del pool y la devuelve
    al salir del bloque (incluso si ocurre una excepción).

    Uso:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(...)
    """
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)
