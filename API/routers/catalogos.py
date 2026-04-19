"""
routers/catalogos.py — Endpoints de catálogo.
Prefix: /catalogos
"""
import logging
from fastapi import APIRouter, HTTPException
from database import get_conn
from queries import listar_entidades, listar_fechas, listar_productos, listar_renglones
from schemas import EntidadResumen, FechasResponse, ProductoCatalogo, RenglonCatalogo

log = logging.getLogger(__name__)
router = APIRouter(prefix="/catalogos", tags=["Catálogos"])


@router.get("/entidades", response_model=list[EntidadResumen],
    summary="Lista de entidades disponibles")
def get_entidades():
    with get_conn() as conn:
        filas = listar_entidades(conn)
    if not filas:
        raise HTTPException(status_code=404, detail="No hay entidades en la base de datos.")
    return [EntidadResumen(**dict(f)) for f in filas]


@router.get("/fechas", response_model=FechasResponse,
    summary="Fechas de corte disponibles",
    description="Si se pasa `codigo_entidad` (número entero), filtra fechas de esa entidad.")
def get_fechas(codigo_entidad: str | None = None):
    with get_conn() as conn:
        fechas = listar_fechas(conn, codigo_entidad)
    if not fechas:
        raise HTTPException(status_code=404, detail="No hay fechas disponibles.")
    return FechasResponse(fechas=fechas)


@router.get("/productos", response_model=list[ProductoCatalogo],
    summary="Productos financieros disponibles (unicap + descripción)",
    description=(
        "Devuelve los productos únicos cargados en la BD. "
        "Usa el texto de `descrip_uc` como parámetro `producto` "
        "en los endpoints de cartera. Ej: 'rotativo', 'vivienda', 'libranza'."
    ))
def get_productos():
    with get_conn() as conn:
        productos = listar_productos(conn)
    if not productos:
        raise HTTPException(status_code=404, detail="No hay productos disponibles.")
    return [ProductoCatalogo(**dict(p)) for p in productos]


@router.get("/renglones", response_model=list[RenglonCatalogo],
    summary="Niveles de renglon disponibles",
    description=(
        "El renglon es un código jerárquico: 5=Total de la línea, "
        "10/15/20/25=Subtotales. Por defecto los endpoints usan renglon=5 (totales)."
    ))
def get_renglones():
    return [RenglonCatalogo(**r) for r in listar_renglones(None)]
