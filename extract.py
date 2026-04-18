"""
extract.py — Fase EXTRACT

Responsabilidades:
  - Leer el archivo CSV de cartera desde disco.
  - Registrar metadatos del archivo (tamaño, hash SHA-256) para auditoría.
  - Normalizar los nombres de columna a snake_case ASCII.
  - Validar que las columnas clave del negocio estén presentes.

No aplica ninguna transformación de datos; eso es responsabilidad de
transform.py.  El DataFrame devuelto refleja el CSV "tal cual", salvo
la normalización de nombres de columna.
"""

import hashlib
import logging
import os
import re
import unicodedata

import pandas as pd

from config import CSV_SEP, UNIQUE_KEYS

log = logging.getLogger(__name__)


# ── Helpers internos ──────────────────────────────────────────────────────────

def _a_snake_case(texto: str) -> str:
    """
    Convierte un encabezado de columna a snake_case ASCII puro.

    Pasos:
      1. Elimina tildes y diacríticos (NFKD + encode ascii).
      2. Convierte a minúsculas.
      3. Reemplaza cualquier secuencia de caracteres no alfanuméricos por '_'.
      4. Elimina guiones bajos al inicio y al final.

    Ejemplos:
      '(18) Calificación de Riesgo A / Saldo' → 'calificacion_de_riesgo_a_saldo'
      'FECHA_CORTE'                            → 'fecha_corte'
      'Número de Clientes Mora > 30 días'      → 'numero_de_clientes_mora_30_dias'
    """
    sin_tildes = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", sin_tildes.lower()).strip("_")


def _log_metadatos_archivo(ruta: str) -> None:
    """
    Registra en el log el tamaño y los primeros 16 caracteres del hash
    SHA-256 del archivo.  Sirve para auditar qué versión del CSV se procesó.
    """
    tam_kb = os.path.getsize(ruta) / 1024
    with open(ruta, "rb") as f:
        sha256 = hashlib.sha256(f.read()).hexdigest()
    log.info(f"Archivo: {tam_kb:,.1f} KB  |  SHA-256: {sha256[:16]}...")


def _validar_columnas_clave(df: pd.DataFrame) -> None:
    """
    Lanza ValueError si alguna columna de UNIQUE_KEYS no existe en el DataFrame.

    Se ejecuta después de normalizar los nombres de columna, por lo que
    compara contra el snake_case resultante.
    """
    faltantes = [c for c in UNIQUE_KEYS if c not in df.columns]
    if faltantes:
        raise ValueError(f"Columnas clave faltantes en el CSV: {faltantes}")


# ── Función principal del módulo ──────────────────────────────────────────────

def extraer_csv(ruta: str) -> pd.DataFrame:
    """
    Lee el CSV de cartera y devuelve un DataFrame con columnas normalizadas.

    Pasos:
      1. Registra metadatos del archivo (tamaño + SHA-256).
      2. Lee el CSV respetando el separador configurado en CSV_SEP
         (autodetección si CSV_SEP es None).
      3. Normaliza los nombres de columna a snake_case ASCII.
      4. Valida la presencia de todas las columnas de UNIQUE_KEYS.

    Args:
        ruta: Ruta al archivo CSV de entrada.

    Returns:
        DataFrame con los datos crudos del CSV y columnas en snake_case.

    Raises:
        FileNotFoundError: Si el archivo no existe en la ruta indicada.
        ValueError: Si faltan columnas clave después de normalizar los nombres.
    """
    log.info(f"[EXTRACT] Leyendo: {ruta}")
    _log_metadatos_archivo(ruta)

    # Separador configurable para evitar el costoso sniffing en archivos grandes.
    if CSV_SEP:
        df = pd.read_csv(ruta, encoding="utf-8", sep=CSV_SEP)
    else:
        df = pd.read_csv(ruta, encoding="utf-8", sep=None, engine="python")

    log.info(f"[EXTRACT] Leído: {len(df):,} filas x {len(df.columns)} columnas")

    # Normalizar encabezados antes de validar para que la comparación
    # sea consistente con UNIQUE_KEYS (que ya están en snake_case).
    df.columns = [_a_snake_case(c) for c in df.columns]

    _validar_columnas_clave(df)

    log.info("[EXTRACT] Extracción completada sin errores")
    return df
