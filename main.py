"""
╔══════════════════════════════════════════════════════════════════════╗
║         RPA — Distribución de Cartera por Producto                   ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║                                                                      ║
║  Estructura del proyecto:                                            ║
║    main.py        ← este archivo: orquesta Extract → Transform → Load║
║    extract.py     ← lectura y validación del CSV                     ║
║    transform.py   ← limpieza, normalización y fusión de filas        ║
║    load.py        ← conexión a PostgreSQL, DDL e inserción           ║
║    config.py      ← variables de entorno y constantes globales       ║
║                                                                      ║
║  Uso    : python main.py                                             ║
║  Config : .env   (ver .env.example)                                  ║
║  Log    : rpa_cartera.log                                            ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import logging

import psycopg2

from config import CSV_PATH, setup_logging
from extract import extraer_csv
from load import conectar, crear_base_de_datos, crear_tabla, insertar_lotes
from transform import transformar

# Inicializa el logger raíz con handlers de archivo y consola.
setup_logging()
log = logging.getLogger(__name__)


def main() -> None:
    """
        1. Crear BD   — garantiza que la base de datos exista antes de operar.
        2. Extract    — lee y valida el CSV de cartera.
        3. Transform  — limpia, normaliza y fusiona el DataFrame.
        4. Load       — crea la tabla (si no existe) e inserta con upsert.

    Ante cualquier error irrecuperable se propaga la excepción después de
    registrarla, de modo que el proceso termina con código de salida ≠ 0
    y puede ser relanzado por el scheduler del RPA.
    """
    log.info("=" * 60)
    log.info("INICIO RPA — Cartera por Producto  v5")
    log.info("=" * 60)

    # ── Paso 1: Crear base de datos si no existe ──────────────────
    crear_base_de_datos()

    # ── Paso 2: Extract — leer CSV ────────────────────────────────
    try:
        df_raw = extraer_csv(CSV_PATH)
    except FileNotFoundError:
        log.error(f"Archivo CSV no encontrado: {CSV_PATH}")
        raise
    except ValueError as exc:
        log.error(f"Error de validación en el CSV: {exc}")
        raise

    # ── Paso 3: Transform — limpiar y normalizar ──────────────────
    df_clean = transformar(df_raw)

    # ── Paso 4: Load — insertar en PostgreSQL ─────────────────────
    try:
        conn = conectar()
    except psycopg2.OperationalError as exc:
        log.error(f"No se pudo conectar a PostgreSQL: {exc}")
        raise

    try:
        with conn:
            crear_tabla(conn, df_clean)
            insertar_lotes(conn, df_clean)
    except Exception as exc:
        log.error(f"Error inesperado durante la carga: {exc}")
        raise
    finally:
        conn.close()
        log.info("Conexión cerrada")

    log.info("=" * 60)
    log.info("RPA finalizado exitosamente")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
