"""
load.py — Fase LOAD
 
Responsabilidades:
  - Crear la base de datos PostgreSQL si no existe (requiere conexión admin).
  - Abrir la conexión de trabajo a PostgreSQL.
  - Generar y ejecutar el DDL CREATE TABLE inferido del DataFrame.
  - Migrar fecha_corte a DATE si la tabla ya existe con tipo TEXT.
  - Insertar los datos en lotes con upsert idempotente (ON CONFLICT DO UPDATE).
  - Registrar en el log cuántas filas fueron insertadas vs actualizadas.
"""
 
import logging
 
import pandas as pd
import psycopg2
from psycopg2 import sql as pgsql
from psycopg2.extras import execute_values
 
from config import BATCH_SIZE, DB_CONFIG, DB_CONFIG_ADMIN, DTYPE_SQL, TABLE_NAME, UNIQUE_KEYS
 
log = logging.getLogger(__name__)
 
 
def _q(nombre: str) -> str:
    """Envuelve un identificador en comillas dobles para PostgreSQL."""
    return f'"{nombre}"'
 
 
def crear_base_de_datos() -> None:
    """
    Crea la base de datos de trabajo si no existe.
    Requiere conexión de administración con autocommit=True.
    """
    nombre_bd = DB_CONFIG["dbname"]
    log.info(f"[LOAD] Verificando existencia de la base de datos '{nombre_bd}'...")
 
    try:
        conn_admin = psycopg2.connect(**DB_CONFIG_ADMIN)
        conn_admin.autocommit = True
 
        with conn_admin.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (nombre_bd,))
            existe = cur.fetchone()
 
        if not existe:
            with conn_admin.cursor() as cur:
                cur.execute(
                    pgsql.SQL("CREATE DATABASE {}").format(pgsql.Identifier(nombre_bd))
                )
            log.info(f"[LOAD] Base de datos '{nombre_bd}' creada correctamente")
        else:
            log.info(f"[LOAD] Base de datos '{nombre_bd}' ya existe, continuando...")
 
    except psycopg2.OperationalError as exc:
        log.error(
            f"[LOAD] No se pudo conectar como administrador a "
            f"'{DB_CONFIG_ADMIN['dbname']}'. Error: {exc}"
        )
        raise
    finally:
        conn_admin.close()
 
 
def conectar() -> psycopg2.extensions.connection:
    """Abre y retorna una conexión a la base de datos de trabajo."""
    log.info(
        f"[LOAD] Conectando a PostgreSQL: "
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} "
        f"(usuario: {DB_CONFIG['user']})"
    )
    return psycopg2.connect(**DB_CONFIG)
 
 
def _migrar_fecha_corte_a_date(conn: psycopg2.extensions.connection) -> None:
    """
    Si la tabla ya existe y fecha_corte está almacenada como TEXT,
    la migra a DATE usando USING fecha_corte::date.
 
    Esto ocurre cuando el ETL fue ejecutado antes del fix de transform.py
    y la columna quedó como TEXT en lugar de DATE.
 
    Si ya es DATE (u otro tipo compatible), no hace nada.
    """
    check_sql = """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_name = %s AND column_name = 'fecha_corte'
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(check_sql, (TABLE_NAME,))
        row = cur.fetchone()
 
    if row is None:
        return  # La tabla no existe aún, no hay nada que migrar
 
    tipo_actual = row[0].lower()
    if tipo_actual in ("date", "timestamp without time zone", "timestamp with time zone"):
        log.info(f"[LOAD] fecha_corte ya es tipo '{tipo_actual}', no se requiere migración")
        return
 
    log.info(f"[LOAD] Migrando fecha_corte de '{tipo_actual}' → DATE...")
    alter_sql = f"""
        ALTER TABLE {TABLE_NAME}
        ALTER COLUMN "fecha_corte" TYPE DATE
        USING "fecha_corte"::date
    """
    with conn.cursor() as cur:
        cur.execute(alter_sql)
    conn.commit()
    log.info("[LOAD] Migración de fecha_corte completada: TEXT → DATE")
 
 
def crear_tabla(conn: psycopg2.extensions.connection, df: pd.DataFrame) -> None:
    """
    Genera y ejecuta el DDL CREATE TABLE IF NOT EXISTS inferido del DataFrame.
 
    Incluye:
      - id SERIAL PRIMARY KEY
      - Una columna por cada columna del DataFrame con tipo SQL inferido
      - UNIQUE (UNIQUE_KEYS) para soportar upsert idempotente
 
    Después de crear (o verificar) la tabla, llama a _migrar_fecha_corte_a_date
    para corregir el tipo si la tabla existía con fecha_corte TEXT.
    """
    definiciones = ["    id  SERIAL PRIMARY KEY"]
    for col, dtype in df.dtypes.items():
        # Python date nativo → pandas dtype 'object', pero queremos DATE en PostgreSQL.
        # Detección explícita por nombre de columna para garantizar el tipo correcto.
        if col == "fecha_corte":
            tipo_sql = "DATE"
        else:
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
 
    # Migrar fecha_corte si la tabla ya existía con tipo TEXT
    _migrar_fecha_corte_a_date(conn)
 
 
def insertar_lotes(conn: psycopg2.extensions.connection, df: pd.DataFrame) -> None:
    """
    Inserta el DataFrame en la tabla destino en lotes con upsert idempotente.
 
    ON CONFLICT (UNIQUE_KEYS) DO UPDATE SET garantiza idempotencia.
    Un único COMMIT al final — rollback total si cualquier lote falla.
    xmax=0 → inserción nueva; xmax≠0 → actualización.
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
 
    # Convertir datetime64 a date nativo (por si acaso queda algún Timestamp).
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
 