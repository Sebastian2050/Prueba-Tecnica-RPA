"""
load.py — Fase LOAD

Responsabilidades:
  - Crear la base de datos PostgreSQL si no existe (requiere conexión admin).
  - Abrir la conexión de trabajo a PostgreSQL.
  - Generar y ejecutar el DDL CREATE TABLE inferido del DataFrame.
  - Insertar los datos en lotes con upsert idempotente (ON CONFLICT DO UPDATE).
  - Registrar en el log cuántas filas fueron insertadas vs actualizadas.

Separar la carga en su propio módulo permite reutilizarla con diferentes
fuentes de datos sin modificar la lógica de extracción o transformación.
"""

import logging

import pandas as pd
import psycopg2
from psycopg2 import sql as pgsql
from psycopg2.extras import execute_values

from config import BATCH_SIZE, DB_CONFIG, DB_CONFIG_ADMIN, DTYPE_SQL, TABLE_NAME, UNIQUE_KEYS

log = logging.getLogger(__name__)


# ── Helpers internos ──────────────────────────────────────────────────────────

def _q(nombre: str) -> str:
    """Envuelve un identificador en comillas dobles para PostgreSQL."""
    return f'"{nombre}"'


# ── Funciones públicas del módulo ─────────────────────────────────────────────

def crear_base_de_datos() -> None:
    """
    Crea la base de datos de trabajo (DB_CONFIG['dbname']) si no existe.

    PostgreSQL no permite CREATE DATABASE dentro de una transacción activa,
    por lo que se conecta a la BD de sistema (DB_CONFIG_ADMIN, normalmente
    'postgres') y usa autocommit=True.

    Si la BD ya existe, la función no hace ningún cambio.

    Raises:
        psycopg2.OperationalError: Si no se puede conectar como administrador.
    """
    nombre_bd = DB_CONFIG["dbname"]
    log.info(f"[LOAD] Verificando existencia de la base de datos '{nombre_bd}'...")

    try:
        conn_admin = psycopg2.connect(**DB_CONFIG_ADMIN)
        conn_admin.autocommit = True  # requerido por CREATE DATABASE

        with conn_admin.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (nombre_bd,))
            existe = cur.fetchone()

        if not existe:
            with conn_admin.cursor() as cur:
                # pgsql.Identifier escapa correctamente el nombre de la BD
                cur.execute(
                    pgsql.SQL("CREATE DATABASE {}").format(pgsql.Identifier(nombre_bd))
                )
            log.info(f"[LOAD] Base de datos '{nombre_bd}' creada correctamente")
        else:
            log.info(f"[LOAD] Base de datos '{nombre_bd}' ya existe, continuando...")

    except psycopg2.OperationalError as exc:
        log.error(
            f"[LOAD] No se pudo conectar como administrador a "
            f"'{DB_CONFIG_ADMIN['dbname']}'. "
            f"Verifique DB_ADMIN_NAME, usuario y contraseña. Error: {exc}"
        )
        raise
    finally:
        conn_admin.close()


def conectar() -> psycopg2.extensions.connection:
    """
    Abre y retorna una conexión a la base de datos de trabajo.

    Returns:
        Objeto connection de psycopg2 listo para usar.

    Raises:
        psycopg2.OperationalError: Si el servidor no está disponible o las
        credenciales son incorrectas.
    """
    log.info(
        f"[LOAD] Conectando a PostgreSQL: "
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} "
        f"(usuario: {DB_CONFIG['user']})"
    )
    return psycopg2.connect(**DB_CONFIG)


def crear_tabla(conn: psycopg2.extensions.connection, df: pd.DataFrame) -> None:
    """
    Genera y ejecuta el DDL CREATE TABLE IF NOT EXISTS inferido del DataFrame.

    La tabla incluye:
      - Una columna 'id' SERIAL como clave primaria surrogate.
      - Una columna por cada columna del DataFrame con el tipo SQL mapeado
        desde DTYPE_SQL (TEXT como fallback para tipos no reconocidos).
      - Una restricción UNIQUE sobre UNIQUE_KEYS para soportar el upsert.

    Si la tabla ya existe, no hace ningún cambio.

    Args:
        conn: Conexión activa a PostgreSQL.
        df:   DataFrame limpio del que se infieren los tipos de columna.
    """
    definiciones = ["    id  SERIAL PRIMARY KEY"]
    for col, dtype in df.dtypes.items():
        tipo_sql = DTYPE_SQL.get(str(dtype), "TEXT")
        definiciones.append(f"    {_q(col):<42} {tipo_sql}")
    definiciones.append(
        f"    UNIQUE ({', '.join(_q(k) for k in UNIQUE_KEYS)})"
    )

    ddl = "CREATE TABLE IF NOT EXISTS {} (\n{}\n);".format(
        TABLE_NAME, ",\n".join(definiciones)
    )

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info(f"[LOAD] Tabla '{TABLE_NAME}' lista")


def insertar_lotes(conn: psycopg2.extensions.connection, df: pd.DataFrame) -> None:
    """
    Inserta el DataFrame en la tabla destino en lotes, con upsert idempotente.

    Estrategia de upsert:
      - ON CONFLICT (UNIQUE_KEYS) DO UPDATE SET ...
        Garantiza que ejecuciones repetidas actualicen registros existentes
        en lugar de fallar con error de unicidad.

    Transaccionalidad:
      - Un único COMMIT al final.  Si cualquier lote falla, el rollback
        deshace TODO lo insertado en esa ejecución, dejando la BD consistente.

    Conteo de inserción vs actualización:
      - Usa el campo del sistema 'xmax' de PostgreSQL para distinguir filas
        recién insertadas (xmax = 0) de filas actualizadas (xmax ≠ 0).

    Args:
        conn: Conexión activa a PostgreSQL.
        df:   DataFrame limpio con las columnas en el mismo orden que la tabla.

    Raises:
        Exception: Cualquier error durante la inserción provoca rollback y
                   re-raise para que main.py lo registre y propague.
    """
    cols     = list(df.columns)
    conflict = ", ".join(_q(k) for k in UNIQUE_KEYS)
    updates  = ", ".join(
        f"{_q(c)} = EXCLUDED.{_q(c)}" for c in cols if c not in UNIQUE_KEYS
    )

    sql = f"""
        INSERT INTO {TABLE_NAME} ({", ".join(_q(c) for c in cols)})
        VALUES %s
        ON CONFLICT ({conflict}) DO UPDATE SET {updates}
        RETURNING (xmax = 0) AS es_insercion
    """

    # psycopg2 no puede adaptar datetime64; convertir a date nativo de Python.
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64"]).columns:
        df[col] = df[col].dt.date

    total        = len(df)
    insertadas   = 0
    actualizadas = 0

    try:
        with conn.cursor() as cur:
            for i in range(0, total, BATCH_SIZE):
                lote      = df.iloc[i: i + BATCH_SIZE]
                registros = [tuple(r) for r in lote.itertuples(index=False, name=None)]
                execute_values(cur, sql, registros, fetch=True)

                resultados    = cur.fetchall()
                ins           = sum(1 for r in resultados if r[0])
                act           = sum(1 for r in resultados if not r[0])
                insertadas   += ins
                actualizadas += act

                log.info(
                    f"[LOAD] Lote {i // BATCH_SIZE + 1}: "
                    f"{min(i + BATCH_SIZE, total):,}/{total:,} filas — "
                    f"+{ins} nuevas / ~{act} actualizadas"
                )

        conn.commit()
        log.info(
            f"[LOAD] Inserción completada  |  "
            f"Nuevas: {insertadas:,}  |  Actualizadas: {actualizadas:,}"
        )

    except Exception:
        conn.rollback()
        log.error("[LOAD] Error durante la inserción — rollback ejecutado")
        raise
