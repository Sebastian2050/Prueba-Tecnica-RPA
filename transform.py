"""
transform.py — Fase TRANSFORM del pipeline ETL.

Responsabilidades:
  - Limpiar y normalizar texto (strip, title-case).
  - Parsear fechas en formato colombiano dd/mm/yyyy.
  - Detectar y convertir columnas numéricas en formato colombiano
    (punto = separador de miles, coma = decimal).
  - Detectar colisiones reales en filas duplicadas y registrarlas en el log.
  - Fusionar filas complementarias por clave de negocio (groupby + max).
  - Eliminar filas con nulos en columnas clave.
  - Reemplazar nulos y negativos en columnas numéricas con 0.

Recibe el DataFrame "crudo" de extract.py y devuelve uno limpio y listo
para ser cargado por load.py.
"""

import logging

import pandas as pd

from config import UNIQUE_KEYS

log = logging.getLogger(__name__)


# ── Helpers internos ──────────────────────────────────────────────────────────

def _limpiar_numero(valor) -> float:
    """
    Parsea un valor en formato numérico colombiano a float de Python.

    Formato colombiano: punto como separador de miles, coma como decimal.
      '1.234.567,89' → 1234567.89
      '0,5'          → 0.5
      ''  / NaN      → 0.0
      'ABC'          → 0.0  (con WARNING en el log)

    Args:
        valor: Cualquier valor de una celda del DataFrame.

    Returns:
        float resultante, o 0.0 si el valor es nulo, vacío o no parseable.
    """
    if pd.isna(valor) or str(valor).strip() == "":
        return 0.0
    try:
        return float(str(valor).strip().replace(".", "").replace(",", "."))
    except ValueError:
        log.warning(f"Valor no numérico ignorado: '{valor}'")
        return 0.0


def _es_columna_numerica(serie: pd.Series) -> bool:
    """
    Determina si una columna de tipo object contiene mayoritariamente números
    en formato colombiano.

    Estrategia: intenta convertir hasta 200 valores no nulos con la misma
    lógica de _limpiar_numero.  Si más del 50 % tienen éxito, la columna
    se clasifica como numérica.

    Usar conversiones reales (en lugar de regex) evita falsos positivos con
    códigos alfanuméricos como 'EP001' o '123-AB' que coinciden con patrones
    numéricos pero fallan al convertirse.

    Args:
        serie: Columna del DataFrame (dtype object).

    Returns:
        True si la columna es mayoritariamente numérica, False en caso contrario.
    """
    muestra = serie.dropna().head(200).astype(str)
    if muestra.empty:
        return False

    def _intenta(v: str) -> bool:
        try:
            float(v.strip().replace(".", "").replace(",", "."))
            return True
        except ValueError:
            return False

    return muestra.apply(_intenta).mean() > 0.5


def _detectar_colisiones(df: pd.DataFrame, cols_num: list[str]) -> None:
    """
    Detecta y registra colisiones reales entre filas duplicadas por UNIQUE_KEYS.

    Una *colisión real* ocurre cuando dos o más filas con idéntica clave de
    negocio tienen valores distintos y ambos son > 0 en la misma columna
    numérica.  En ese caso, la fusión posterior (max) elegirá un valor de
    forma arbitraria, lo que puede distorsionar los datos.

    Este método NO lanza excepción: el pipeline continúa pero deja registro
    auditado en el log para que el equipo de datos investigue la fuente.

    Args:
        df:       DataFrame antes de la fusión de duplicados.
        cols_num: Lista de columnas numéricas a inspeccionar.
    """
    duplicados = df[df.duplicated(subset=UNIQUE_KEYS, keep=False)]
    if duplicados.empty:
        log.info("[TRANSFORM] Sin filas duplicadas por clave de negocio")
        return

    n_colisiones = 0
    for clave, grupo in duplicados.groupby(UNIQUE_KEYS, sort=False):
        if len(grupo) < 2:
            continue
        for col in cols_num:
            valores_reales = grupo[col][grupo[col] > 0]
            if valores_reales.nunique() > 1:
                n_colisiones += 1
                log.warning(
                    f"COLISION real en columna '{col}' "
                    f"para clave {dict(zip(UNIQUE_KEYS, clave))}: "
                    f"valores={valores_reales.tolist()} → se conservará el máximo"
                )

    if n_colisiones == 0:
        log.info("[TRANSFORM] Sin colisiones reales detectadas en la fusión")
    else:
        log.warning(
            f"[TRANSFORM] Total colisiones reales: {n_colisiones}. "
            "Revisar CSV de origen."
        )


# ── Función principal del módulo ──────────────────────────────────────────────

def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas las transformaciones de limpieza al DataFrame extraído.

    Secuencia de pasos (en orden):
      1.  strip()      — elimina espacios en todas las columnas de texto.
      2.  title()      — capitaliza columnas de texto que NO son clave.
      3.  Fechas       — parsea 'fecha_corte' a datetime (formato dd/mm/yyyy).
      4.  Números      — detecta y convierte columnas numéricas en formato
                         colombiano de object → float64.
      5.  Colisiones   — detecta y loguea conflictos en filas duplicadas.
      6.  Fusión       — agrupa por UNIQUE_KEYS; max() para numéricos,
                         first() para texto.
      7.  Nulos clave  — elimina filas donde alguna columna clave es nula.
      8.  Nulos num.   — reemplaza NaN en columnas numéricas con 0.0.
      9.  Negativos    — reemplaza valores < 0 con 0.0.
      10. fecha_corte  — garantiza dtype datetime64[ns] post-groupby (pandas
                         puede degradarlo a object en algunas versiones).

    Args:
        df: DataFrame crudo devuelto por extract.extraer_csv().

    Returns:
        DataFrame limpio, sin duplicados, listo para la fase de carga.
    """
    log.info("[TRANSFORM] Iniciando transformaciones...")

    # ── 1 & 2. Limpiar texto ──────────────────────────────────────
    cols_object = df.select_dtypes(include=["object"]).columns.tolist()

    # strip en todas las columnas de texto (incluyendo claves)
    for col in cols_object:
        df[col] = df[col].str.strip()

    # title-case solo en columnas NO clave para no alterar los identificadores
    for col in (c for c in cols_object if c not in UNIQUE_KEYS):
        df[col] = df[col].str.title()

    # ── 3. Convertir fechas ───────────────────────────────────────
    df["fecha_corte"] = pd.to_datetime(
        df["fecha_corte"], format="%d/%m/%Y", errors="coerce"
    )
    n_invalidas = df["fecha_corte"].isna().sum()
    if n_invalidas:
        log.warning(f"[TRANSFORM] {n_invalidas} fechas inválidas en 'fecha_corte'")

    # ── 4. Convertir columnas numéricas en formato colombiano ─────
    # Detectar solo entre las columnas que siguen siendo object después de
    # la conversión de fechas.
    cols_obj_actuales = df.select_dtypes(include=["object"]).columns.tolist()
    cols_numericas_str = [c for c in cols_obj_actuales if _es_columna_numerica(df[c])]

    for col in cols_numericas_str:
        df[col] = df[col].apply(_limpiar_numero)

    log.info(f"[TRANSFORM] Columnas numéricas convertidas: {len(cols_numericas_str)}")

    # ── 5. Detectar colisiones reales antes de fusionar ───────────
    cols_num = df.select_dtypes(include=["float64", "int64"]).columns.tolist()
    _detectar_colisiones(df, cols_num)

    # ── 6. Fusionar filas complementarias ─────────────────────────
    # Las filas con la misma clave de negocio suelen complementarse:
    # una puede tener valores en columnas donde la otra tiene 0 o NaN.
    # max() conserva el mayor valor no nulo por columna.
    n_antes = len(df)
    cols_texto_extra = [
        c for c in df.columns if c not in cols_num and c not in UNIQUE_KEYS
    ]
    agg = {c: "max" for c in cols_num}
    agg.update({c: "first" for c in cols_texto_extra})

    df = df.groupby(UNIQUE_KEYS, as_index=False).agg(agg)
    log.info(f"[TRANSFORM] Filas fusionadas (duplicados eliminados): {n_antes - len(df):,}")

    # ── 7. Eliminar filas con claves nulas ────────────────────────
    n_antes = len(df)
    df = df.dropna(subset=UNIQUE_KEYS)
    log.info(f"[TRANSFORM] Filas eliminadas por claves nulas: {n_antes - len(df):,}")

    # ── 8. Rellenar nulos numéricos con 0 ─────────────────────────
    # Recalcular cols_num porque groupby puede cambiar los dtypes.
    cols_num = df.select_dtypes(include=["float64", "int64"]).columns.tolist()
    df[cols_num] = df[cols_num].fillna(0)

    # ── 9. Corregir negativos → 0 ─────────────────────────────────
    n_negativos = (df[cols_num] < 0).sum().sum()
    if n_negativos:
        df[cols_num] = df[cols_num].clip(lower=0)
        log.warning(f"[TRANSFORM] Valores negativos corregidos a 0: {n_negativos}")

    # ── 10. Garantizar dtype datetime64 en fecha_corte ────────────
    # Algunas versiones de pandas degradan datetime64 → object en groupby.
    if df["fecha_corte"].dtype == object:
        df["fecha_corte"] = pd.to_datetime(df["fecha_corte"], errors="coerce")
        log.info("[TRANSFORM] fecha_corte reconvertida a datetime64 tras groupby")

    log.info(f"[TRANSFORM] Transformación completada → {len(df):,} filas limpias")
    return df.reset_index(drop=True)
