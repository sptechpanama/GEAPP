from __future__ import annotations

COL_FECHA = "Fecha"
COL_DESC = "Descripcion"
COL_CONCEPTO = "Concepto"
COL_MONTO = "Monto"
COL_CATEGORIA = "Categoria"
COL_ESCENARIO = "Escenario"
COL_PROYECTO = "Proyecto"
COL_CLIENTE_ID = "ClienteID"
COL_CLIENTE_NOMBRE = "ClienteNombre"
COL_EMPRESA = "Empresa"
COL_POR_COBRAR = "Por_cobrar"
COL_COBRADO = "Cobrado"
COL_FECHA_COBRO = "Fecha de cobro"
COL_ROW_ID = "RowID"
COL_USUARIO = "Usuario"

COL_POR_PAGAR = "Por_pagar"
COL_PROVEEDOR = "Proveedor"
COL_FECHA_PAGO = "Fecha esperada de pago"

INGRESOS_BASE_COLUMNS = [
    COL_FECHA,
    COL_DESC,
    COL_CONCEPTO,
    COL_MONTO,
    COL_CATEGORIA,
    COL_ESCENARIO,
    COL_PROYECTO,
    COL_CLIENTE_ID,
    COL_CLIENTE_NOMBRE,
    COL_EMPRESA,
    COL_POR_COBRAR,
    COL_COBRADO,
    COL_FECHA_COBRO,
    COL_ROW_ID,
    COL_USUARIO,
]

GASTOS_BASE_COLUMNS = [
    COL_FECHA,
    COL_DESC,
    COL_CONCEPTO,
    COL_MONTO,
    COL_CATEGORIA,
    COL_ESCENARIO,
    COL_PROYECTO,
    COL_CLIENTE_ID,
    COL_CLIENTE_NOMBRE,
    COL_EMPRESA,
    COL_POR_PAGAR,
    COL_PROVEEDOR,
    COL_FECHA_PAGO,
    COL_ROW_ID,
    COL_USUARIO,
]

MISC_CATEGORY_ALIASES = {
    "miscelaneos",
    "miscelaneos.",
    "miscelaneo",
    "miscelaneo",
    "miscelaneos y otros",
    "miscelaneos/otros",
    "miscelaneos-otros",
    "misc",
    "miscellaneous",
    "miscel\u00e1neos",
    "miscel\u00e1neo",
}

COMISION_CATEGORY_ALIASES = {
    "comisiones",
    "comision",
    "comisi\u00f3n",
    "comisiones venta",
    "comisiones ventas",
}

EMPRESA_ALL_LABEL = "Todas"
