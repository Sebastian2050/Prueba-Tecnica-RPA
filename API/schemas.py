from datetime import date
from decimal import Decimal
from pydantic import BaseModel, field_validator


class EntidadResumen(BaseModel):
    tipo_entidad:        int
    tipo_entidad_nombre: str
    codigo_entidad:      int
    nombreentidad:       str
    model_config = {"from_attributes": True}


class ProductoCatalogo(BaseModel):
    unicap:     int
    descrip_uc: str
    model_config = {"from_attributes": True}


class RenglonCatalogo(BaseModel):
    renglon:     int
    descripcion: str
    model_config = {"from_attributes": True}


class FechasResponse(BaseModel):
    fechas: list[date]


class CarteraItem(BaseModel):
    tipo_entidad:   int
    codigo_entidad: int
    nombreentidad:  str
    fecha_corte:    date | str
    unicap:         int  | None = None
    descrip_uc:     str  | None = None
    renglon:        int  | None = None
    desc_renglon:   str  | None = None

    saldo_total:          float | None = None
    saldo_vigente:        float | None = None
    vencida_1_2_meses:    float | None = None
    vencida_2_3_meses:    float | None = None
    vencida_1_3_meses:    float | None = None
    vencida_3_4_meses:    float | None = None
    vencida_mas_4_meses:  float | None = None
    vencida_3_6_meses:    float | None = None
    vencida_mas_6_meses:  float | None = None
    vencida_1_4_meses:    float | None = None
    vencida_4_6_meses:    float | None = None
    vencida_6_12_meses:   float | None = None
    vencida_12_18_meses:  float | None = None
    vencida_mas_12_meses: float | None = None
    vencida_mas_18_meses: float | None = None
    clientes_mora_30_dias: float | None = None
    cal_a_clientes:       float | None = None
    cal_a_saldo:          float | None = None
    cal_b_clientes:       float | None = None
    cal_b_saldo:          float | None = None
    cal_c_clientes:       float | None = None
    cal_c_saldo:          float | None = None
    cal_d_clientes:       float | None = None
    cal_d_saldo:          float | None = None
    cal_e_clientes:       float | None = None
    cal_e_saldo:          float | None = None

    model_config = {"from_attributes": True}

    @field_validator("*", mode="before")
    @classmethod
    def coerce_types(cls, v):
        if isinstance(v, Decimal):
            return float(v)
        return v


class CarteraEntidadResponse(BaseModel):
    entidad:        str
    tipo_entidad:   int
    codigo_entidad: int
    fecha_corte:    date | str
    total_registros: int
    registros:      list[CarteraItem]


class CarteraProductoResponse(BaseModel):
    producto:        str
    renglon:         int
    total_registros: int
    registros:       list[CarteraItem]
