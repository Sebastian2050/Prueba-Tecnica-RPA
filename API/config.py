"""
config.py — Configuración global de la API de Cartera.

Carga variables de entorno desde .env y expone las constantes
que usan los demás módulos.  Mismo patrón que el pipeline ETL:
un único lugar para cambiar cualquier valor sin tocar la lógica.

Variables de entorno reconocidas:
    DB_HOST        host PostgreSQL          (default: localhost)
    DB_PORT        puerto TCP               (default: 5432)
    DB_NAME        base de datos            (default: cartera_db)
    DB_USER        usuario                  (default: postgres)
    DB_PASSWORD    contraseña               (default: 1234)
    TABLE_NAME     tabla de cartera         (default: cartera_por_producto)
    API_HOST       host de escucha          (default: 0.0.0.0)
    API_PORT       puerto de escucha        (default: 8000)
    API_RELOAD     hot-reload en desarrollo (default: false)
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Base de datos ─────────────────────────────────────────────────────────────
DB_CONFIG: dict[str, str | int] = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "cartera_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "1234"),
}

TABLE_NAME: str = os.getenv("TABLE_NAME", "cartera_por_producto")

# ── Servidor ──────────────────────────────────────────────────────────────────
API_HOST:   str  = os.getenv("API_HOST",   "0.0.0.0")
API_PORT:   int  = int(os.getenv("API_PORT", "8000"))
API_RELOAD: bool = os.getenv("API_RELOAD", "false").lower() == "true"

# ── Columnas de identificación de negocio ─────────────────────────────────────
# Deben coincidir exactamente con las del pipeline ETL.
UNIQUE_KEYS: list[str] = [
    "tipo_entidad",
    "codigo_entidad",
    "nombreentidad",
    "fecha_corte",
    "unicap",
    "renglon",
]
