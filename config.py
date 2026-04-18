"""
config.py — Configuración global del pipeline ETL.

Carga variables de entorno desde el archivo .env (si existe) y expone
todas las constantes que usan los demás módulos.  Centralizar la config
aquí facilita cambios sin tocar la lógica de negocio.

Variables de entorno reconocidas (.env.example):
    DB_HOST        host del servidor PostgreSQL          (default: localhost)
    DB_PORT        puerto TCP                            (default: 5432)
    DB_NAME        nombre de la base de datos de trabajo (default: cartera_db)
    DB_USER        usuario de conexión                   (default: postgres)
    DB_PASSWORD    contraseña                            (default: 1234)
    DB_ADMIN_NAME  BD de sistema para CREATE DATABASE    (default: postgres)
    CSV_PATH       ruta al archivo CSV de entrada        (default: ver abajo)
    TABLE_NAME     nombre de la tabla destino            (default: cartera_por_producto)
    BATCH_SIZE     filas por lote en el INSERT           (default: 5000)
    CSV_SEP        separador del CSV; vacío = autodetect (default: None)
"""

import logging
import os

from dotenv import load_dotenv

# Carga .env si existe; si no, usa los valores por defecto definidos abajo.
load_dotenv()

# ── Conexión a la base de datos de trabajo ────────────────────────────────────
DB_CONFIG: dict[str, str | int] = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "cartera_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "1234"),
}

# ── Conexión de administración para CREATE DATABASE ───────────────────────────
# PostgreSQL no permite CREATE DATABASE dentro de una transacción activa, por
# lo que se requiere conectar a una BD de sistema ('postgres' por defecto).
DB_CONFIG_ADMIN: dict[str, str | int] = {
    **DB_CONFIG,
    "dbname": os.getenv("DB_ADMIN_NAME", "postgres"),
}

# ── Parámetros del pipeline ───────────────────────────────────────────────────
CSV_PATH:   str       = os.getenv("CSV_PATH",   "Distribución_de_cartera_por_producto_20260415.csv")
TABLE_NAME: str       = os.getenv("TABLE_NAME", "cartera_por_producto")
BATCH_SIZE: int       = int(os.getenv("BATCH_SIZE", "5000"))

# Separador del CSV.  None activa la autodetección de pandas (más lenta en
# archivos grandes). Definir un valor explícito (ej. "," o ";") mejora el
# rendimiento en producción.
CSV_SEP: str | None   = os.getenv("CSV_SEP") or None

# ── Clave de negocio ──────────────────────────────────────────────────────────
# Columnas que identifican de forma única un registro de cartera.
# Se usan en: validación, fusión de filas duplicadas y cláusula UNIQUE de PostgreSQL.
UNIQUE_KEYS: list[str] = [
    "tipo_entidad",
    "codigo_entidad",
    "nombreentidad",
    "fecha_corte",
    "unicap",
    "renglon",
]

# ── Mapeo de tipos pandas → SQL ───────────────────────────────────────────────
DTYPE_SQL: dict[str, str] = {
    "int64":          "INTEGER",
    "float64":        "NUMERIC",
    "bool":           "BOOLEAN",
    "object":         "TEXT",
    "datetime64[ns]": "DATE",
}


def setup_logging() -> None:
    """
    Configura el logger raíz con dos handlers:
      - FileHandler  → rpa_cartera.log  (persistente, UTF-8)
      - StreamHandler → consola          (para monitoreo en tiempo real)

    Debe llamarse una sola vez desde main.py antes de importar los demás
    módulos, para que todos hereden la misma configuración.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("rpa_cartera.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
