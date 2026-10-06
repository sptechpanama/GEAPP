from __future__ import annotations

from functools import partial

import pandas as pd
import streamlit as st

from services.finance_financing import (
    FactoringTerms,
    cheapest,
    cost_curve,
    credit_balance,
    credit_cost,
    credit_terms_from_row,
    factoring_cost,
    private_cost,
)


@st.fragment
def render_financing_comparison(lineas_provider, ingresos: pd.DataFrame, gastos: pd.DataFrame, empresa: str = "Todas") -> None:
    st.caption("Marca las opciones disponibles. Simulación sin abonos de capital, con interés simple y meses de 30 días; no registra movimientos.")
    credit_column, private_column, factoring_column = st.columns(3)
    use_credit = credit_column.checkbox("Línea de crédito", key="fin_compare_credit")
    use_private = private_column.checkbox("Inversionistas privados", key="fin_compare_private")
    use_factoring = factoring_column.checkbox("Factoring", key="fin_compare_factoring")
    amount_column, months_column = st.columns(2)
    amount = amount_column.number_input("Monto a financiar (USD netos)", min_value=0.0, value=0.0, step=100.0, key="fin_compare_amount")
    months = months_column.number_input("Plazo estimado (meses)", min_value=0.0, max_value=24.0, value=3.0, step=0.5, key="fin_compare_months")
    calculators = {}
    if use_private:
        st.caption("Inversionistas privados: 2.5% mensual simple sobre el monto financiado, sin comisiones adicionales incluidas.")
        calculators["Inversionistas privados"] = partial(private_cost, amount)
    if use_credit:
        try:
            lines = lineas_provider().copy()
            if not {"Activa", "Empresa", "Nombre linea", "RowID"}.issubset(lines.columns):
                raise ValueError("No hay una configuración válida de líneas de crédito.")
            lines = lines.loc[lines["Activa"].astype(str).str.strip().str.lower().isin(["si", "sí", "true", "1"])]
            if empresa != "Todas":
                lines = lines.loc[lines["Empresa"].astype(str).str.strip().str.upper().eq(empresa.upper())]
            if lines.empty:
                st.warning("No hay líneas activas para la empresa seleccionada. Configúrala en Gestionar línea de crédito.")
            else:
                rows = lines.to_dict("records")
                selected = st.selectbox(
                    "Línea configurada",
                    range(len(rows)),
                    format_func=lambda index: f"{rows[index]['Empresa']} · {rows[index]['Nombre linea']} · {rows[index].get('Banco', '')}",
                    key="fin_compare_line",
                )
                row = rows[selected]
                effective = pd.to_datetime(row.get("Fecha vigencia tasa"), errors="coerce")
                if pd.isna(effective) or effective.date() > pd.Timestamp.now(tz="America/Panama").date():
                    raise ValueError("La línea no tiene una fecha de tasa vigente verificada. Actualiza su configuración.")
                terms = credit_terms_from_row(row, credit_balance(row, ingresos, gastos))
                if terms.daily_pct == 0:
                    raise ValueError("La tasa diaria está en cero. Confirma y configura la tasa real antes de comparar.")
                with st.expander("Condiciones de la línea", expanded=False):
                    st.caption(
                        f"Tasa: {terms.daily_pct:.6f}% diario · Cupo estimado: ${terms.available:,.2f} · "
                        f"Desembolso: ${terms.disbursement_fee:,.2f} · Cargos mensuales: ${terms.monthly_fees:,.2f}."
                    )
                    allocate_annual = st.checkbox("Distribuir costos anuales proporcionalmente al plazo", key="fin_compare_annual")
                    st.caption(f"Costos anuales configurados: ${terms.annual_cost:,.2f}. No se incluyen por defecto: pueden ser costos ya pagados de una línea existente.")
                calculators["Línea de crédito"] = partial(credit_cost, amount, terms=terms, allocate_annual=allocate_annual)
        except Exception as exc:
            st.warning(f"Línea de crédito no comparada: {exc}")
    if use_factoring:
        with st.expander("Condiciones de factoring", expanded=True):
            st.caption("Finanzas ya registra factoring sin recurso: recibido + retenido + comisión inicial = factura. No tiene una tarifa automática; ingresa la cotización real.")
            rate_column, commission_column, advance_column = st.columns(3)
            rate = rate_column.number_input("Tasa mensual simple (%)", min_value=0.0, value=None, step=0.1, key="fin_compare_factoring_rate")
            commission = commission_column.number_input("Comisión inicial (% de factura)", min_value=0.0, max_value=100.0, value=None, step=0.1, key="fin_compare_factoring_commission")
            advance = advance_column.number_input("Anticipo (% de factura)", min_value=0.0, max_value=100.0, value=None, step=1.0, key="fin_compare_factoring_advance")
            basis = st.selectbox("La tasa mensual se aplica sobre", ["factura", "anticipo"], index=None, format_func=lambda value: "Valor de la factura" if value == "factura" else "Anticipo antes de comisiones", key="fin_compare_factoring_basis")
            fixed_fee = st.number_input("Cargo fijo adicional (USD)", min_value=0.0, value=0.0, step=1.0, key="fin_compare_factoring_fixed")
            st.caption("Supuesto: la comisión inicial se descuenta del anticipo; el interés se paga al cierre, no se descuenta por adelantado. La retención se recupera y no cuenta como gasto. Si tu contrato difiere, hay que ajustar esta regla.")
        if any(value is None for value in (rate, commission, advance, basis)):
            st.warning("Factoring pendiente de condiciones: no se trata como una opción gratuita ni se incluye todavía en el ranking.")
        else:
            terms = FactoringTerms(rate, commission, advance, basis, fixed_fee)
            calculators["Factoring"] = partial(factoring_cost, amount, terms=terms)
    if not (use_credit or use_private or use_factoring):
        st.info("Selecciona al menos una opción disponible para comparar.")
        return
    if amount <= 0:
        st.info("Ingresa el monto a financiar.")
        return
    estimates = []
    valid = {}
    for name, calculate in calculators.items():
        try:
            estimate = calculate(months)
        except ValueError as exc:
            st.warning(f"{name}: {exc}")
            continue
        estimates.append(estimate)
        valid[name] = calculate
    if not estimates:
        return
    winners = cheapest(estimates)
    if len(estimates) > 1:
        st.success(f"Menor costo total a {months:g} meses: {' / '.join(winners)} · ${min(cost.total for cost in estimates):,.2f}.")
    else:
        st.info("Solo hay una opción con condiciones completas; no se puede elegir entre alternativas todavía.")
    least_interest = min(estimate.interest for estimate in estimates)
    interest_winners = [estimate.option for estimate in estimates if estimate.interest == least_interest]
    st.caption(f"Menor interés: {' / '.join(interest_winners)} · ${least_interest:,.2f} (sin comisiones ni cargos).")
    table = pd.DataFrame([
        {"Opción": estimate.option, "Intereses (USD)": estimate.interest, "Comisiones y cargos (USD)": estimate.charges, "Costo total (USD)": estimate.total}
        for estimate in sorted(estimates, key=lambda value: value.total)
    ])
    st.dataframe(table, hide_index=True, use_container_width=True, column_config={
        column: st.column_config.NumberColumn(column, format="dollar") for column in table.columns if column != "Opción"
    })
    st.line_chart(cost_curve(valid), x_label="Plazo (meses)", y_label="Intereses + cargos acumulados (USD)")
    st.caption("Gráfica de 0 a 8 meses. En el mes 0 ya cuentan los cargos de desembolso. La elección usa costo total, no solo intereses: evita favorecer una opción con comisiones altas.")
    for estimate in estimates:
        if estimate.invoice_amount is not None:
            st.caption(f"Factoring: para recibir ${amount:,.2f} netos, factura necesaria ${estimate.invoice_amount:,.2f}; retenido ${estimate.retained:,.2f} (no es costo). Requiere una cuenta por cobrar elegible; no financia automáticamente una compra previa a facturar.")
