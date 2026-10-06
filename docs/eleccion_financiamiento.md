# Elección de financiamiento

Dentro de Finanzas, junto a **Resumen financiero**, el comparador permite marcar línea de crédito, inversionistas privados y factoring. Monto y plazo son una simulación: no crean ingresos, gastos ni deudas en Sheets. Los controles recalculan mediante un fragmento de Streamlit, sin volver a ejecutar toda la página en cada ajuste.

## Cálculos

- **Privados:** monto neto × 2.5% × meses. Interés simple, sin capitalización ni comisiones adicionales informadas.
- **Línea:** monto × tasa diaria configurada × 30 × meses. Se agregan cargo por desembolso y cargos mensuales de `LineasCredito`. El cupo estimado descuenta capital desembolsado menos capital pagado de la misma empresa y línea. No equivale a una aprobación del banco.
- **Costos anuales:** excluidos por defecto, porque pueden estar pagados para una línea existente. La casilla de condiciones permite distribuirlos proporcionalmente: (límite × cargo anual % + seguros anuales) × meses / 12. Es una asignación de costos, no una predicción de fechas de cobro del banco.
- **Factoring:** actualmente Finanzas registra operaciones sin recurso y sus comisiones inicial/final en importes, pero no guarda una tarifa mensual. No se deduce una tasa de operaciones históricas ni se usa cero como sustituto de un dato faltante.

Para factoring se ingresan tasa mensual, base de interés (factura o anticipo), comisión inicial sobre factura, porcentaje adelantado y cargo fijo adicional. La simulación supone comisión descontada al desembolsar e interés pagado al cierre, sin capitalización. Si el contrato cobra interés anticipado, mínimos o escalas, es necesario ajustar el modelo antes de usarlo.

Para comparar la misma liquidez inicial:

`Factura necesaria = (monto neto requerido + cargo fijo) / (porcentaje de anticipo − porcentaje de comisión inicial)`

`Retenido = factura × (1 − porcentaje de anticipo)`

El retenido no es un gasto. Factoring exige una cuenta por cobrar elegible: no sustituye necesariamente a crédito para fabricar o comprar antes de facturar. Las condiciones introducidas son del escenario en pantalla; no modifican la configuración ni los registros financieros existentes.

## Lectura

La tabla separa intereses, comisiones/cargos y costo total. Se identifica explícitamente la opción con **menos intereses**; la recomendación económica compara **costo total**, no solo la tasa anunciada. Hay gráfica de 0–8 meses, con los cargos iniciales incluidos desde el mes 0. El plazo seleccionado puede ser mayor que el horizonte de la gráfica. No hay amortizaciones intermedias; cada mes simulado equivale a 30 días.

Una opción incompleta, con tasa bancaria guardada en cero, con tasa futura o que exceda el cupo no se compara. Las demás siguen disponibles. Con una sola opción no se afirma que sea la mejor. Las tasas y condiciones deben contrastarse con la cotización o contrato vigente antes de contratar.

## Validación

`python -m pytest tests/test_finance_financing.py tests/test_finance_financing_view.py`

Incluye fórmulas, redondeo monetario, cambios de opción por plazo, comisiones, retenciones, cupos por empresa, condiciones faltantes y pruebas de interacción de Streamlit.
