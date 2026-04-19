"""
routers/entidades.py — Cartera por entidad.
Prefix: /entidades
"""
import logging
from datetime import date
from fastapi import APIRouter, HTTPException, Query
from database import get_conn
from queries import cartera_por_entidad, existe_entidad
from schemas import CarteraEntidadResponse, CarteraItem

log = logging.getLogger(__name__)
router = APIRouter(prefix="/entidades", tags=["Cartera por Entidad"])


@router.get(
    "/{codigo_entidad}/cartera",
    response_model=CarteraEntidadResponse,
    summary="Cartera de una entidad",
    description=(
        "Devuelve la cartera de la entidad identificada por `codigo_entidad` (número entero). "
        "Sin `fecha_corte` usa la fecha más reciente. "
        "Filtra por producto con `producto` (texto parcial de descrip_uc, ej: 'rotativo', 'vivienda'). "
        "Filtra por nivel jerárquico con `renglon` (5=totales, 10/15/20/25=subtotales)."
    ),
)
def get_cartera_entidad(
    codigo_entidad: str,
    fecha_corte: date | None = Query(default=None, example="2026-03-31"),
    producto: str | None = Query(default=None, description="Texto parcial del producto. Ej: 'rotativo'", example="rotativo"),
    renglon: int | None = Query(default=None, description="Nivel de renglon: 5, 10, 15, 20 o 25", example=5),
):
    with get_conn() as conn:
        if not existe_entidad(conn, codigo_entidad):
            raise HTTPException(status_code=404,
                detail=f"Entidad con código '{codigo_entidad}' no encontrada. Consulta /catalogos/entidades para ver los códigos disponibles.")
        filas = cartera_por_entidad(conn, codigo_entidad, fecha_corte, producto, renglon)

    if not filas:
        raise HTTPException(status_code=404,
            detail="No hay datos con los filtros indicados. Consulta /catalogos/productos para ver los productos disponibles.")

    primera = filas[0]
    return CarteraEntidadResponse(
        entidad=primera["nombreentidad"],
        tipo_entidad=primera["tipo_entidad"],
        codigo_entidad=primera["codigo_entidad"],
        fecha_corte=primera["fecha_corte"],
        total_registros=len(filas),
        registros=[CarteraItem(**dict(f)) for f in filas],
    )
