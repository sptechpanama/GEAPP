from __future__ import annotations

import uuid
from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

from core.finance_v2 import (
    GlobalFilters,
    apply_global_filters,
    build_analisis_gerencial,
    build_balance_general_simplificado,
    build_cashflow_actual,
    build_cashflow_proyectado,
    build_cuentas_por_cobrar,
    build_cuentas_por_pagar,
    build_estado_resultados,
    format_money_es,
    format_number_es,
    format_percent_es,
    get_filter_options,
    get_finance_sheet_config,
    load_finance_inputs,
    normalize_gastos,
    normalize_ingresos,
    split_real_vs_pending,
)
from core.finance_v2.constants import COL_FECHA
from ui.theme import apply_global_theme


st.set_page_config(page_title="Finanzas 2", page_icon="\U0001f4c8", layout="wide")
apply_global_theme()

if st.session_state.get("authentication_status") is not True:
    st.switch_page("Inicio.py")



def _safe_rerun() -> None:
    rerun = getattr(st, "rerun", None)
    if callable(rerun):
        rerun()
        return
    legacy = getattr(st, "experimental_rerun", None)
    if callable(legacy):
        legacy()



def _render_kpi_row(metrics: list[tuple[str, str]], cols: int = 5) -> None:
    if not metrics:
        return
    for i in range(0, len(metrics), cols):
        row = metrics[i : i + cols]
        columns = st.columns(len(row))
        for col, (label, value) in zip(columns, row):
            with col:
                st.metric(label, value)



def _line_chart(df: pd.DataFrame, x: str, y: str, title: str, color: str = "#22c55e"):
    if df.empty:
        st.info("Sin datos para mostrar en este grafico.")
        return
    chart = (
        alt.Chart(df)
        .mark_line(point=True, color=color)
        .encode(
            x=alt.X(f"{x}:T", title="Fecha"),
            y=alt.Y(f"{y}:Q", title=title),
            tooltip=[alt.Tooltip(f"{x}:T", title="Fecha"), alt.Tooltip(f"{y}:Q", title=title, format=",.2f")],
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)



def _bar_chart(df: pd.DataFrame, x: str, y: str, title: str, color: str = "#0ea5e9"):
    if df.empty:
        st.info("Sin datos para mostrar en este grafico.")
        return
    chart = (
        alt.Chart(df)
        .mark_bar(color=color)
        .encode(
            x=alt.X(f"{x}:N", sort="-y", title=x.replace("_", " ").title()),
            y=alt.Y(f"{y}:Q", title=title),
            tooltip=[alt.Tooltip(f"{x}:N", title=x.title()), alt.Tooltip(f"{y}:Q", title=title, format=",.2f")],
        )
        .properties(height=320)
    )
    st.altair_chart(chart, use_container_width=True)



def _series_to_csv_download(df: pd.DataFrame, filename: str, label: str):
    csv_data = df.to_csv(index=False).encode("utf-8")
    st.download_button(label, data=csv_data, file_name=filename, mime="text/csv")


st.title("Finanzas 2")
st.caption(
    "Vista gerencial y analitica construida sobre los mismos datos de Finanzas 1. "
    "No reemplaza ni modifica el flujo operativo actual."
)

if "finanzas2_cache_token" not in st.session_state:
    st.session_state["finanzas2_cache_token"] = uuid.uuid4().hex

try:
    cfg = get_finance_sheet_config()
except Exception as exc:
    st.error(f"No se pudo leer configuracion de hojas: {exc}")
    st.stop()

with st.spinner("Cargando datos financieros..."):
    data = load_finance_inputs(
        cfg["sheet_id"],
        cfg["ws_ing"],
        cfg["ws_gas"],
        st.session_state["finanzas2_cache_token"],
    )

df_ing = normalize_ingresos(data.get("ingresos", pd.DataFrame()))
df_gas = normalize_gastos(data.get("gastos", pd.DataFrame()))

combined_dates = pd.concat([df_ing[COL_FECHA], df_gas[COL_FECHA]], ignore_index=True).dropna()
if combined_dates.empty:
    min_date = date(date.today().year, 1, 1)
    max_date = date.today()
else:
    min_date = combined_dates.min().date()
    max_date = combined_dates.max().date()

opts = get_filter_options(df_ing, df_gas)

with st.sidebar:
    st.markdown("### Filtros globales")
    fecha_desde = st.date_input("Desde", value=min_date, min_value=min_date, max_value=max_date, key="f2_desde")
    fecha_hasta = st.date_input("Hasta", value=max_date, min_value=min_date, max_value=max_date, key="f2_hasta")

    empresa_opt = ["Todas"] + opts["empresas"]
    empresa = st.selectbox("Empresa", options=empresa_opt, index=0, key="f2_empresa")

    escenarios_opts = opts["escenarios"]
    escenarios_sel = st.multiselect(
        "Escenario",
        options=escenarios_opts,
        default=escenarios_opts,
        key="f2_escenarios",
    )

    search = st.text_input("Busqueda", key="f2_search", placeholder="cliente, proyecto, categoria, proveedor...")

    vista_modo = st.radio("Vista", ["Consolidado", "Por empresa"], horizontal=False, key="f2_vista")
    granularidad = st.selectbox("Flujo proyectado", ["D", "W", "M"], index=0, key="f2_gran")

    include_misc = st.toggle(
        "Incluir Miscelaneos en rentabilidad",
        value=False,
        help="Se aplica a Estado de resultados y analisis gerencial. Caja y balance mantienen todos los movimientos.",
        key="f2_include_misc",
    )

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Actualizar tablero", key="f2_refresh"):
            st.session_state["finanzas2_cache_token"] = uuid.uuid4().hex
            _safe_rerun()
    with col_b:
        if st.button("Limpiar filtros", key="f2_clear"):
            for k in ["f2_empresa", "f2_search", "f2_escenarios", "f2_gran", "f2_vista", "f2_include_misc"]:
                st.session_state.pop(k, None)
            st.session_state["f2_desde"] = min_date
            st.session_state["f2_hasta"] = max_date
            _safe_rerun()

if fecha_desde > fecha_hasta:
    st.warning("El rango de fechas es invalido.")
    st.stop()

filters = GlobalFilters(
    fecha_desde=fecha_desde,
    fecha_hasta=fecha_hasta,
    empresa=empresa,
    busqueda=search,
    escenarios=escenarios_sel,
)

ing_f, gas_f = apply_global_filters(df_ing, df_gas, filters)
split = split_real_vs_pending(ing_f, gas_f)

cash_actual = build_cashflow_actual(split["ing_real"], split["gas_real"])
cxc_df = build_cuentas_por_cobrar(split["ing_pend"])
cxp_df, cxp_quality = build_cuentas_por_pagar(split["gas_pend"])

cxc_total = float(cxc_df["monto"].sum()) if not cxc_df.empty else 0.0
cxp_total = float(cxp_df["monto"].sum()) if not cxp_df.empty else 0.0

proyectado = build_cashflow_proyectado(
    split["ing_pend"],
    split["gas_pend"],
    saldo_inicial=cash_actual["metricas"]["efectivo_actual"],
    granularidad=granularidad,
)

estado = build_estado_resultados(ing_f, gas_f, include_miscelaneos=include_misc)
balance = build_balance_general_simplificado(
    efectivo_actual=cash_actual["metricas"]["efectivo_actual"],
    cuentas_por_cobrar=cxc_total,
    cuentas_por_pagar=cxp_total,
)

analisis = build_analisis_gerencial(ing_f, gas_f, cxc_df, include_miscelaneos=include_misc)

metricas_resumen = [
    ("Efectivo actual", format_money_es(cash_actual["metricas"]["efectivo_actual"])),
    ("Flujo neto del periodo", format_money_es(cash_actual["metricas"]["flujo_neto"])),
    ("Cuentas por cobrar", format_money_es(cxc_total)),
    ("Cuentas por pagar", format_money_es(cxp_total)),
    ("Capital de trabajo", format_money_es(balance["metricas"]["capital_trabajo"])),
    ("Utilidad del periodo", format_money_es(estado["metricas"]["utilidad_operativa"])),
    ("Margen del periodo", format_percent_es(estado["metricas"]["margen_operativo"])),
    ("Saldo proyectado final", format_money_es(proyectado["metricas"]["saldo_proyectado_final"])),
    ("Posicion financiera neta", format_money_es(balance["metricas"]["posicion_financiera_neta"])),
]

st.markdown("## Resumen Ejecutivo")
_render_kpi_row(metricas_resumen, cols=3)

resumen_df = pd.DataFrame(metricas_resumen, columns=["Indicador", "Valor"])
with st.sidebar:
    _series_to_csv_download(resumen_df, "finanzas2_resumen.csv", "Exportar resumen")

st.caption(
    "Reglas base: flujo de caja en base a cobrado/pagado; estado de resultados gerencial (no contable completo); "
    "balance general simplificado con informacion disponible."
)

tab_a, tab_b, tab_c, tab_d, tab_e, tab_f, tab_g = st.tabs(
    [
        "A. Resumen Ejecutivo",
        "B. Flujo de Caja Actual",
        "C. Flujo de Caja Proyectado",
        "D. Estado de Resultados",
        "E. Balance General",
        "F. Cuentas por Cobrar y por Pagar",
        "G. Analisis Gerencial",
    ]
)

with tab_a:
    st.markdown("### Estado general")
    st.write(
        f"Rango analizado: **{fecha_desde.isoformat()}** a **{fecha_hasta.isoformat()}** | "
        f"Empresa: **{empresa}** | Vista: **{vista_modo}**"
    )

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Tension de liquidez")
        tension = cxc_total - cxp_total
        st.metric("Cuentas por cobrar - por pagar", format_money_es(tension))
        if tension < 0:
            st.warning("La cartera pendiente no cubre las obligaciones pendientes en el periodo filtrado.")
        elif tension == 0:
            st.info("Cartera pendiente y obligaciones pendientes estan equilibradas.")
        else:
            st.success("La cartera pendiente supera las obligaciones pendientes.")

    with col2:
        st.markdown("#### Calidad de proyeccion")
        st.write(f"Pagos pendientes con fecha fallback: **{cxp_quality['con_fallback_fecha']}**")
        st.write(f"Pagos pendientes sin fecha usable: **{cxp_quality['sin_fecha']}**")
        if cxp_quality["con_fallback_fecha"] > 0:
            st.caption("Supuesto aplicado: cuando no existe fecha estimada de pago, se usa Fecha del registro del gasto.")

with tab_b:
    st.markdown("### Flujo de caja actual")
    st.caption("Base caja: ingresos cobrados (Por_cobrar=No) y gastos pagados (Por_pagar=No).")

    kpis_actual = [
        ("Entradas reales", format_money_es(cash_actual["metricas"]["entradas_reales"])),
        ("Salidas reales", format_money_es(cash_actual["metricas"]["salidas_reales"])),
        ("Flujo neto", format_money_es(cash_actual["metricas"]["flujo_neto"])),
        ("Saldo acumulado", format_money_es(cash_actual["metricas"]["efectivo_actual"])),
    ]
    _render_kpi_row(kpis_actual, cols=4)

    serie_actual = cash_actual["serie"]
    if not serie_actual.empty:
        c1, c2 = st.columns([2, 1])
        with c1:
            _line_chart(serie_actual, COL_FECHA, "saldo", "Saldo acumulado", color="#22c55e")
        with c2:
            _bar_chart(
                serie_actual.tail(25).assign(fecha_str=lambda d: d[COL_FECHA].dt.strftime("%Y-%m-%d")),
                "fecha_str",
                "flujo",
                "Flujo diario",
                color="#0ea5e9",
            )
    else:
        st.info("No hay movimientos reales en el periodo filtrado.")

    if vista_modo == "Por empresa" and not cash_actual["movimientos"].empty:
        st.markdown("#### Flujo por empresa")
        por_empresa = (
            cash_actual["movimientos"].groupby("Empresa", as_index=False)["flujo"].sum().sort_values("flujo", ascending=False)
        )
        _bar_chart(por_empresa.rename(columns={"Empresa": "empresa"}), "empresa", "flujo", "Flujo neto por empresa", color="#22c55e")

with tab_c:
    st.markdown("### Flujo de caja proyectado")
    st.caption(
        "Incluye cobros pendientes y pagos pendientes futuros. Si no existe fecha estimada de pago, "
        "se usa la Fecha del registro como fallback y se reporta en calidad de datos."
    )

    kpis_proj = [
        ("Saldo inicial", format_money_es(proyectado["metricas"]["saldo_inicial"])),
        ("Cobros futuros", format_money_es(proyectado["metricas"]["cobros_futuros"])),
        ("Pagos futuros", format_money_es(proyectado["metricas"]["pagos_futuros"])),
        ("Flujo neto proyectado", format_money_es(proyectado["metricas"]["flujo_neto_proyectado"])),
        ("Saldo final proyectado", format_money_es(proyectado["metricas"]["saldo_proyectado_final"])),
    ]
    _render_kpi_row(kpis_proj, cols=5)

    for note in proyectado.get("notas", []):
        st.caption(f"- {note}")

    if not proyectado["serie"].empty:
        _line_chart(
            proyectado["serie"].rename(columns={"fecha_evento": "fecha"}),
            "fecha",
            "saldo_proyectado",
            "Saldo proyectado",
            color="#f59e0b",
        )
        st.markdown("#### Eventos futuros")
        st.dataframe(proyectado["eventos"], use_container_width=True, hide_index=True)
    else:
        st.info("No hay eventos de cobro/pago pendientes para proyectar en el rango filtrado.")

with tab_d:
    st.markdown("### Estado de resultados (gerencial)")
    for note in estado.get("notas", []):
        st.caption(f"- {note}")

    st.dataframe(
        estado["estado"],
        use_container_width=True,
        hide_index=True,
        column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
    )

    if not estado["mensual"].empty:
        st.markdown("#### Evolucion mensual")
        df_m = estado["mensual"].copy().melt(id_vars=["Mes"], value_vars=["Ingresos", "Gastos", "Utilidad"], var_name="Rubro", value_name="Monto")
        chart = (
            alt.Chart(df_m)
            .mark_line(point=True)
            .encode(
                x=alt.X("Mes:T", title="Mes"),
                y=alt.Y("Monto:Q", title="Monto"),
                color=alt.Color("Rubro:N", title="Rubro"),
                tooltip=["Mes:T", "Rubro:N", alt.Tooltip("Monto:Q", format=",.2f")],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)

    if not estado["por_empresa"].empty:
        st.markdown("#### Desglose por empresa")
        st.dataframe(
            estado["por_empresa"],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ingresos": st.column_config.NumberColumn("Ingresos", format="$%0.2f"),
                "Gastos": st.column_config.NumberColumn("Gastos", format="$%0.2f"),
                "Utilidad": st.column_config.NumberColumn("Utilidad", format="$%0.2f"),
            },
        )

with tab_e:
    st.markdown("### Balance general (simplificado)")
    for note in balance.get("notas", []):
        st.caption(f"- {note}")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("#### Activos")
        st.dataframe(
            balance["activos"],
            use_container_width=True,
            hide_index=True,
            column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
        )
    with c2:
        st.markdown("#### Pasivos")
        st.dataframe(
            balance["pasivos"],
            use_container_width=True,
            hide_index=True,
            column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
        )
    with c3:
        st.markdown("#### Patrimonio")
        st.dataframe(
            balance["patrimonio"],
            use_container_width=True,
            hide_index=True,
            column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
        )

    st.markdown("#### Totales")
    totals = pd.DataFrame(
        [
            {"Indicador": "Total activos", "Monto": balance["metricas"]["total_activos"]},
            {"Indicador": "Total pasivos", "Monto": balance["metricas"]["total_pasivos"]},
            {"Indicador": "Patrimonio neto estimado", "Monto": balance["metricas"]["patrimonio_neto"]},
            {"Indicador": "Capital de trabajo", "Monto": balance["metricas"]["capital_trabajo"]},
        ]
    )
    st.dataframe(
        totals,
        use_container_width=True,
        hide_index=True,
        column_config={"Monto": st.column_config.NumberColumn("Monto", format="$%0.2f")},
    )

with tab_f:
    st.markdown("### Cuentas por cobrar y por pagar")

    cxc_cols, cxp_cols = st.columns(2)
    with cxc_cols:
        st.markdown("#### Cuentas por cobrar")
        st.metric("Total CxC", format_money_es(cxc_total))
        st.dataframe(
            cxc_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "monto": st.column_config.NumberColumn("Monto", format="$%0.2f"),
                "fecha_esperada_cobro": st.column_config.DateColumn("Fecha esperada"),
            },
        )
    with cxp_cols:
        st.markdown("#### Cuentas por pagar")
        st.metric("Total CxP", format_money_es(cxp_total))
        st.dataframe(
            cxp_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "monto": st.column_config.NumberColumn("Monto", format="$%0.2f"),
                "fecha_esperada_pago": st.column_config.DateColumn("Fecha esperada"),
            },
        )

    st.caption(
        f"Calidad CxP: fallback de fecha en {cxp_quality['con_fallback_fecha']} registros; "
        f"sin fecha util en {cxp_quality['sin_fecha']} registros."
    )

with tab_g:
    st.markdown("### Analisis gerencial")

    if not analisis["por_empresa"].empty:
        st.markdown("#### Ingresos, gastos y utilidad por empresa")
        st.dataframe(
            analisis["por_empresa"],
            use_container_width=True,
            hide_index=True,
            column_config={
                "ingresos": st.column_config.NumberColumn("Ingresos", format="$%0.2f"),
                "gastos": st.column_config.NumberColumn("Gastos", format="$%0.2f"),
                "utilidad": st.column_config.NumberColumn("Utilidad", format="$%0.2f"),
            },
        )

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Top categorias de gasto")
        _bar_chart(analisis["top_gastos_categoria"], "categoria", "gasto", "Gasto")

    with c2:
        st.markdown("#### Concentracion de cuentas por cobrar")
        _bar_chart(analisis["concentracion_cxc"].head(10), "cliente", "monto", "Monto CxC", color="#f59e0b")

    st.markdown("#### Evolucion mensual")
    evo = analisis["evolucion_mensual"]
    if not evo.empty:
        evo_long = evo.melt(id_vars=["mes"], value_vars=["ingresos", "gastos", "utilidad"], var_name="serie", value_name="monto")
        chart_evo = (
            alt.Chart(evo_long)
            .mark_line(point=True)
            .encode(
                x=alt.X("mes:T", title="Mes"),
                y=alt.Y("monto:Q", title="Monto"),
                color="serie:N",
                tooltip=["mes:T", "serie:N", alt.Tooltip("monto:Q", format=",.2f")],
            )
            .properties(height=320)
        )
        st.altair_chart(chart_evo, use_container_width=True)
    else:
        st.info("Sin datos para evolucion mensual.")

    st.markdown("#### Concentracion de ingresos por cliente")
    st.dataframe(
        analisis["concentracion_cliente"].head(15),
        use_container_width=True,
        hide_index=True,
        column_config={
            "ingresos": st.column_config.NumberColumn("Ingresos", format="$%0.2f"),
            "participacion_pct": st.column_config.NumberColumn("Participacion %", format="%0.2f"),
        },
    )

    st.markdown("#### Concentracion de ingresos por proyecto")
    st.dataframe(
        analisis["concentracion_proyecto"].head(15),
        use_container_width=True,
        hide_index=True,
        column_config={
            "ingresos": st.column_config.NumberColumn("Ingresos", format="$%0.2f"),
        },
    )

st.markdown("---")
st.caption(
    "Finanzas 2 prioriza lectura gerencial. Finanzas 1 se mantiene intacta para captura/operacion. "
    "Para un modelo contable mas robusto faltaria: calendario de pagos formal, catalogo contable y clasificacion de costos directos estandarizada."
)
