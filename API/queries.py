"""
queries.py — Capa de acceso a datos (DAL).

Estructura real de los datos:
  - codigo_entidad → INTEGER (ej: 49, 7, 1)
  - tipo_entidad   → INTEGER (1=Banco, 2=Corporación, 4=CF, 22=Especial, 32=Cooperativa)
  - renglon        → INTEGER jerárquico (5=Total línea, 10/15/20/25=Subtotales)
  - desc_renglon   → TEXT, descripción del renglon — varía por entidad y unicap
  - unicap         → INTEGER, unidad de captura (producto financiero)
  - descrip_uc     → TEXT, nombre del producto (ej: "CRÉDITO ROTATIVO")
  - fecha_corte    → TEXT en BD, cast ::date para comparar
"""

import logging
from datetime import date

from psycopg2.extras import RealDictCursor

from config import TABLE_NAME

log = logging.getLogger(__name__)

_COLS_ID = """
    tipo_entidad,
    codigo_entidad,
    nombreentidad,
    fecha_corte,
    unicap,
    descrip_uc,
    renglon,
    desc_renglon
"""

_COLS_SALDO = """
    COALESCE("1_saldo_de_la_cartera_a_la_fecha_de_corte_del_reporte", 0)  AS saldo_total,
    COALESCE("2_vigente", 0)                                               AS saldo_vigente,
    COALESCE("3_vencida_1_2_meses", 0)                                     AS vencida_1_2_meses,
    COALESCE("4_vencida_2_3_meses", 0)                                     AS vencida_2_3_meses,
    COALESCE("5_vencida_1_3_meses", 0)                                     AS vencida_1_3_meses,
    COALESCE("6_vencida_3_4_meses", 0)                                     AS vencida_3_4_meses,
    COALESCE("7_vencida_de_4_meses", 0)                                    AS vencida_mas_4_meses,
    COALESCE("8_vencida_3_6_meses", 0)                                     AS vencida_3_6_meses,
    COALESCE("9_vencida_6_meses", 0)                                       AS vencida_mas_6_meses,
    COALESCE("10_vencida_1_4_meses", 0)                                    AS vencida_1_4_meses,
    COALESCE("11_vencida_4_6_meses", 0)                                    AS vencida_4_6_meses,
    COALESCE("12_vencida_6_12_meses", 0)                                   AS vencida_6_12_meses,
    COALESCE("13_vencida_12_18_meses", 0)                                  AS vencida_12_18_meses,
    COALESCE("14_vencida_12_meses", 0)                                     AS vencida_mas_12_meses,
    COALESCE("15_vencida_18_meses", 0)                                     AS vencida_mas_18_meses,
    COALESCE("16_numero_de_clientes_mora_30_dias", 0)                      AS clientes_mora_30_dias,
    COALESCE("17_calificacion_de_riesgo_a_numero_de_clientes", 0)          AS cal_a_clientes,
    COALESCE("18_calificacion_de_riesgo_a_saldo", 0)                       AS cal_a_saldo,
    COALESCE("19_calificacion_de_riesgo_b_numero_de_clientes", 0)          AS cal_b_clientes,
    COALESCE("20_calificacion_de_riesgo_b_saldo", 0)                       AS cal_b_saldo,
    COALESCE("21_calificacion_de_riesgo_c_numero_de_clientes", 0)          AS cal_c_clientes,
    COALESCE("22_calificacion_de_riesgo_c_saldo", 0)                       AS cal_c_saldo,
    COALESCE("23_calificacion_de_riesgo_d_numero_de_clientes", 0)          AS cal_d_clientes,
    COALESCE("24_calificacion_de_riesgo_d_saldo", 0)                       AS cal_d_saldo,
    COALESCE("25_calificacion_de_riesgo_e_numero_de_clientes", 0)          AS cal_e_clientes,
    COALESCE("26_calificacion_de_riesgo_e_saldo", 0)                       AS cal_e_saldo
"""

# Mapa de tipo_entidad int → nombre legible
TIPOS_ENTIDAD = {
    1:  "Banco",
    2:  "Corporación Financiera",
    4:  "Compañía de Financiamiento",
    22: "Entidad Especial",
    32: "Cooperativa Financiera",
}


def _parse_int(value: str) -> int | None:
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None


# ── Catálogos ─────────────────────────────────────────────────────────────────

def listar_entidades(conn) -> list[dict]:
    """Entidades únicas con nombre legible del tipo."""
    sql = f"""
        SELECT DISTINCT
            tipo_entidad,
            codigo_entidad,
            nombreentidad
        FROM {TABLE_NAME}
        ORDER BY tipo_entidad, nombreentidad
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    # Agregar nombre legible del tipo
    for r in rows:
        r["tipo_entidad_nombre"] = TIPOS_ENTIDAD.get(r["tipo_entidad"], str(r["tipo_entidad"]))
    return rows


def listar_fechas(conn, codigo_entidad: str | None = None) -> list[date]:
    if codigo_entidad:
        cod_int = _parse_int(codigo_entidad)
        sql = f"""
            SELECT DISTINCT fecha_corte::date AS fecha_corte
            FROM {TABLE_NAME}
            WHERE codigo_entidad = %s
            ORDER BY 1 DESC
        """
        params = (cod_int if cod_int is not None else codigo_entidad,)
    else:
        sql = f"""
            SELECT DISTINCT fecha_corte::date AS fecha_corte
            FROM {TABLE_NAME}
            ORDER BY 1 DESC
        """
        params = ()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [r["fecha_corte"] for r in cur.fetchall()]


def listar_productos(conn) -> list[dict]:
    """
    Catálogo de productos (unicap + descrip_uc), únicos y ordenados.
    Esto es lo realmente útil para filtrar — no renglon.
    """
    sql = f"""
        SELECT DISTINCT unicap, descrip_uc
        FROM {TABLE_NAME}
        ORDER BY unicap
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


def listar_renglones(conn) -> list[dict]:
    """
    Niveles de renglon disponibles:
      5  = Total de la línea
      10 = Primer subtotal
      15 = Segundo subtotal
      20 = Tercer subtotal
      25 = Cuarto subtotal
    """
    return [
        {"renglon": 5,  "descripcion": "Total de la línea (renglon 5)"},
        {"renglon": 10, "descripcion": "Subtotal nivel 1 (renglon 10)"},
        {"renglon": 15, "descripcion": "Subtotal nivel 2 (renglon 15)"},
        {"renglon": 20, "descripcion": "Subtotal nivel 3 (renglon 20)"},
        {"renglon": 25, "descripcion": "Subtotal nivel 4 (renglon 25)"},
    ]


def existe_entidad(conn, codigo_entidad: str) -> bool:
    cod_int = _parse_int(codigo_entidad)
    sql = f"SELECT 1 FROM {TABLE_NAME} WHERE codigo_entidad = %s LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, (cod_int if cod_int is not None else codigo_entidad,))
        return cur.fetchone() is not None


# ── Consultas principales ─────────────────────────────────────────────────────

def cartera_por_entidad(
    conn,
    codigo_entidad: str,
    fecha_corte: date | None = None,
    producto: str | None = None,   # filtra por descrip_uc (texto parcial)
    renglon: int | None = None,    # filtra por nivel de renglon (5, 10, 15...)
) -> list[dict]:
    cod_int = _parse_int(codigo_entidad)
    conditions = ["codigo_entidad = %s"]
    params: list = [cod_int if cod_int is not None else codigo_entidad]

    if fecha_corte is None:
        conditions.append(
            f"fecha_corte::date = (SELECT MAX(fecha_corte::date) FROM {TABLE_NAME} WHERE codigo_entidad = %s)"
        )
        params.append(cod_int if cod_int is not None else codigo_entidad)
    else:
        conditions.append("fecha_corte::date = %s")
        params.append(fecha_corte)

    if producto:
        conditions.append("LOWER(descrip_uc) LIKE LOWER(%s)")
        params.append(f"%{producto}%")

    if renglon is not None:
        conditions.append("renglon = %s")
        params.append(renglon)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT {_COLS_ID}, {_COLS_SALDO}
        FROM {TABLE_NAME}
        WHERE {where}
        ORDER BY fecha_corte::date DESC, unicap ASC, renglon ASC
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def cartera_por_producto(
    conn,
    producto: str,                  # búsqueda parcial en descrip_uc
    fecha_corte: date | None = None,
    tipo_entidad: int | None = None,
    renglon: int = 5,               # default 5 = totales de cada línea
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    conditions = ["LOWER(descrip_uc) LIKE LOWER(%s)", "renglon = %s"]
    params: list = [f"%{producto}%", renglon]

    if fecha_corte is None:
        conditions.append(
            f"fecha_corte::date = (SELECT MAX(fecha_corte::date) FROM {TABLE_NAME})"
        )
    else:
        conditions.append("fecha_corte::date = %s")
        params.append(fecha_corte)

    if tipo_entidad is not None:
        conditions.append("tipo_entidad = %s")
        params.append(tipo_entidad)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT {_COLS_ID}, {_COLS_SALDO}
        FROM {TABLE_NAME}
        WHERE {where}
        ORDER BY fecha_corte::date DESC, nombreentidad ASC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()
