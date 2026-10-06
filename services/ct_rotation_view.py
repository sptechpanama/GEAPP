from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from services import ct_rotation as rotation


@st.cache_data(ttl=60, max_entries=2, show_spinner=False)
def _snapshot(sheet_id: str) -> dict:
    from sheets import get_client

    client, _ = get_client()
    return rotation.load_snapshot(client, sheet_id=sheet_id)


@st.fragment(run_every="60s")
def render_rotation_view(actor: str, sheet_id: str = rotation.SHEET_ID) -> None:
    st.subheader("Rotación e inventario · Ficha 43358")
    try:
        snapshot = _snapshot(sheet_id)
        st.session_state["ct_rotation_last_snapshot"] = snapshot
    except Exception:
        snapshot = st.session_state.get("ct_rotation_last_snapshot")
        st.warning("No se pudo actualizar la lectura. Se conserva el último histórico disponible.")
        if snapshot is None:
            return
    today = rotation.now().date()
    status = snapshot["status"]
    stamp = status.get("ultimo_exito", "")
    if stamp:
        st.caption(f"Última captura: {stamp[:19].replace('T', ' ')} · hora de Panamá. Lectura automática cada 60 segundos.")
    if status.get("error"):
        st.warning("La última captura no fue completa; se conserva el histórico confirmado.")
    try:
        summary = rotation.rotation_summary(snapshot["records"], today=today)
    except (ValueError, TypeError):
        st.error("El histórico contiene un dato no válido. No se modificaron el histórico ni el inventario.")
        return
    purchases, inventory = st.tabs(["Próximas solicitudes por entidad", "Gestión de inventario"])
    with purchases:
        columns = st.columns(2)
        columns[0].metric("Actos promedio mensuales", f"{summary['acts_per_month']:,.2f}")
        columns[1].metric("Kits promedio mensuales", f"{summary['kits_per_month']:,.0f}")
        st.caption("Promedios desde enero de 2025 hasta hoy, sin duplicar actos derivados, republicaciones verificadas ni cancelaciones. La frecuencia usa todo el histórico de cada hospital.")
        if summary["forecast"].empty:
            st.info("El histórico aún no tiene solicitudes verificadas.")
        else:
            forecast = summary["forecast"].copy()
            forecast["Frecuencia promedio (meses)"] = pd.to_numeric(forecast["Frecuencia promedio (meses)"], errors="coerce").round(2)
            st.dataframe(forecast, hide_index=True, width="stretch", column_config={
                "Última solicitud": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Frecuencia promedio (meses)": st.column_config.NumberColumn(format="localized", help="Promedio de días entre solicitudes distintas dividido entre 30.4375. Con una sola solicitud no se estima."),
                "Próxima solicitud estimada": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "Días hasta la fecha estimada": st.column_config.NumberColumn(format="localized", help="Un número negativo indica que la fecha estimada ya pasó; no significa que exista una compra anunciada."),
            })
            st.caption("Ordenadas por fecha estimada, incluidas las que ya pasaron. Es una estimación histórica, no un calendario confirmado; cambia al registrarse nuevas solicitudes.")
    with inventory:
        saved = snapshot["inventory"]
        if saved is not None:
            try:
                rotation.inventory_plan(saved["kits"], date.fromisoformat(saved["fecha"]), 0, today=today)
            except (ValueError, TypeError, KeyError):
                st.warning("El inventario guardado tiene una cantidad o fecha inválida. Vuelve a registrar el conteo real.")
                saved = None
        with st.form("ct_rotation_inventory_form"):
            columns = st.columns(2)
            stock = columns[0].number_input("Inventario de kits", min_value=0, value=int(float(saved["kits"])) if saved else 0, step=1)
            stock_date = columns[1].date_input("Fecha del inventario", value=date.fromisoformat(saved["fecha"]) if saved else today, max_value=today)
            submitted = st.form_submit_button("Guardar inventario", type="primary")
        if submitted:
            try:
                from sheets import get_client

                client, _ = get_client()
                saved = rotation.save_inventory(client, stock, stock_date, actor, sheet_id=sheet_id)
                snapshot["inventory"] = saved
                st.session_state["ct_rotation_last_snapshot"] = snapshot
                _snapshot.clear()
                st.success("Inventario guardado.")
            except Exception:
                st.error("No se pudo guardar el inventario. Se conserva el registro anterior.")
        st.caption("Supuestos fijos: ganar el 60% de los actos, con cantidades representativas del promedio; un mes de fabricación en China + un mes de tránsito; 6,000 kits por embarque.")
        if saved is None:
            st.info("Guarda el inventario real a la fecha para calcular la reposición.")
            return
        if not summary["kits_per_month"]:
            st.info("Hace falta demanda histórica verificada para calcular la reposición.")
            return
        plan = rotation.inventory_plan(saved["kits"], date.fromisoformat(saved["fecha"]), summary["kits_per_month"], today=today)
        columns = st.columns(3)
        columns[0].metric("Consumo estimado mensual · 60%", f"{plan['monthly_consumption']:,.0f} kits")
        columns[1].metric("Inventario estimado hoy", f"{plan['estimated_stock']:,.0f} kits")
        columns[2].metric("Cobertura estimada", f"{plan['coverage_months']:,.1f} meses")
        order = plan["order_date"]
        when = "Ahora" if order <= today else order.strftime("%d/%m/%Y")
        st.markdown(f"**Pedir a más tardar:** {when} · **Embarques necesarios ahora:** {plan['shipments_now']:,}")
        st.caption(f"Cada embarque de 6,000 kits cubre aproximadamente {plan['shipment_coverage_months']:,.1f} meses. El inventario estimado descuenta el consumo previsto desde la fecha registrada; no sustituye el conteo real.")
