"""
routers/tipos.py — Cartera por producto (tipo de cartera).
Prefix: /productos
"""
import logging
from datetime import date
from fastapi import APIRouter, HTTPException, Query
from database import get_conn
from queries import cartera_por_producto, TIPOS_ENTIDAD
from schemas import CarteraProductoResponse, CarteraItem

log = logging.getLogger(__name__)
router = APIRouter(prefix="/productos", tags=["Cartera por Producto"])


@router.get(
    "/{producto}",
    response_model=CarteraProductoResponse,
    summary="Cartera de un producto en todas las entidades",
    description=(
        "Busca por texto parcial en el nombre del producto (descrip_uc). "
        "Ej: 'rotativo' encuentra 'CRÉDITO ROTATIVO'. "
        "Por defecto devuelve renglon=5 (totales de cada línea). "
        "Filtra por tipo de entidad con `tipo_entidad`: "
        "1=Banco, 2=Corporación Financiera, 4=Compañía de Financiamiento, "
        "22=Entidad Especial, 32=Cooperativa Financiera."
    ),
)
def get_cartera_por_producto(
    producto: str,
    fecha_corte: date | None = Query(default=None, example="2026-03-31"),
    tipo_entidad: int | None = Query(default=None, description="1=Banco, 2=Corp.Fin., 4=CF, 22=Especial, 32=Cooperativa", example=1),
    renglon: int = Query(default=5, description="Nivel jerárquico: 5=totales, 10/15/20/25=subtotales", example=5),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    with get_conn() as conn:
        filas = cartera_por_producto(conn, producto, fecha_corte, tipo_entidad, renglon, limit, offset)

    if not filas:
        tipos_str = ", ".join(f"{k}={v}" for k, v in TIPOS_ENTIDAD.items())
        raise HTTPException(status_code=404,
            detail=f"No hay datos para el producto '{producto}'. "
                   f"Consulta /catalogos/productos para ver los productos disponibles. "
                   f"Tipos de entidad: {tipos_str}.")

    return CarteraProductoResponse(
        producto=producto,
        renglon=renglon,
        total_registros=len(filas),
        registros=[CarteraItem(**dict(f)) for f in filas],
    )
