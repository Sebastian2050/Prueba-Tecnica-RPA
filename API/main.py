"""
main.py — Punto de entrada de la API de Cartera v1.
"""
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import database
from config import API_HOST, API_PORT, API_RELOAD
from routers import catalogos, entidades, tipos

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("api_cartera.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("=" * 60)
    log.info("INICIO API — Cartera por Producto  v1")
    log.info("=" * 60)
    database.startup()
    yield
    database.shutdown()
    log.info("API apagada limpiamente")


app = FastAPI(
    title="API de Cartera por Producto",
    description=(
        "API REST para consultar la distribución de cartera financiera "
        "por entidad y tipo de producto (Superfinanciera de Colombia).\n\n"
        "**Flujo recomendado:**\n"
        "1. `/catalogos/entidades` → ver entidades y sus códigos numéricos\n"
        "2. `/catalogos/productos` → ver productos disponibles (usar en filtros)\n"
        "3. `/catalogos/fechas` → ver fechas de corte disponibles\n"
        "4. `/catalogos/renglones` → entender la jerarquía de renglones\n"
        "5. `/entidades/{codigo}/cartera` → cartera de una entidad\n"
        "6. `/productos/{producto}` → un producto en todas las entidades\n\n"
        "**Tipos de entidad:** 1=Banco, 2=Corp.Financiera, 4=Cía.Financiamiento, "
        "22=Entidad Especial, 32=Cooperativa"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def handler_general(request: Request, exc: Exception):
    log.error(f"Error no controlado en {request.url}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"codigo": 500, "mensaje": "Error interno del servidor.", "detalle": str(exc)},
    )


@app.get("/health", tags=["Sistema"], summary="Estado de la API")
def health():
    return {"estado": "ok", "version": "1.0.0"}


app.include_router(catalogos.router)
app.include_router(entidades.router)
app.include_router(tipos.router)

if __name__ == "__main__":
    uvicorn.run("main:app", host=API_HOST, port=API_PORT, reload=API_RELOAD)
