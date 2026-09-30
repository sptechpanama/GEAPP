# Verificación CT RIR / ficha 43358 — 30 de septiembre de 2026

## Conclusión al corte

Se identificaron tres oportunidades vigentes de la ficha 43358. Las tres estaban en
`cl_abiertas_ct_rir` y en `cl_abiertas_rir_con_ct`, sin marca de descarte. El registro
del orquestador conserva la notificación en ambas etapas: programada y abierta.
No se identificó una omisión de 43358 en el universo de detalles examinado.

| Acto | Unidad de compra | Unidades | Referencia del acto | Último día publicado | Aviso programada | Aviso abierta |
|---|---|---:|---:|---|---|---|
| 2026-1-10-01-08-CL-051614 | Hospital Regional de Chepo | 300 | $7,500.00 | 01/10/2026 | 25/09 18:12 | 28/09 08:09 |
| 2026-1-10-01-08-CL-051598 | Policlínica Joaquín José Vallarino | 150 | $3,750.00 | 01/10/2026 | 25/09 18:12 | 28/09 08:09 |
| 2026-0-12-20-06-CL-042544 | Hospital Regional Dr. Cecilio A. Castillero | 300 | $7,500.00 | 02/10/2026 | 28/09 18:11 | 29/09 08:12 |

Fechas y horas en Panamá. “Aviso” significa registrado por el sistema después de
aceptación SMTP, no una comprobación de llegada a la bandeja de entrada o lectura.
Las fechas de cierre de estos detalles públicos no incluyen hora exacta; no se afirma
que el cierre real sea a medianoche.

## Cómo se verificó

1. Captura independiente de los estados públicos 8 (CL abiertas), 15 (CL programadas)
   y 36 (licitaciones vigentes/nuevas o modificadas que vigila RIR1), con ventana
   amplia para no eliminar programadas futuras. Resultado: **675 + 219 + 368 = 1,262**
   actos únicos. Ninguna ventana alcanzó el límite de 5,000.
2. Lectura de los **1,262 detalles**, sin depender de la selección de Sheets.
   **26** detalles no admitían la ruta tradicional; fueron recuperados mediante
   `/ps/documentos-proceso/pliego-general/publico/get-page`. Cero fallos pendientes
   en los detalles de ese listado. La captura terminó a las **12:15:57** y la recuperación
   de la ruta alternativa se efectuó después; los estados pueden cambiar durante una auditoría.
3. Reejecución del detector real sobre títulos, descripciones y renglones de todos
   esos detalles: tres coincidencias 43358, las mismas presentes en CT RIR.
   No se usaron códigos de clasificación como evidencia de ficha.
4. Para 11 candidatos relacionados con circuitos/anestesia, se descargaron **39 adjuntos,
   179 páginas**. Se extrajo el texto y se aplicó OCR local a 106 páginas escaneadas.
   Se repitió OCR con rotación para dos páginas laterales. Otras 21 páginas sin texto
   reconocido son reversos vacíos o casi vacíos comprobados visualmente.
5. Evidencia 43358: Chepo, requerimientos `12625916` p.1 y ficha adjunta `12626098`
   pp.1,3–4; Vallarino, requerimientos `12620400` p.1; Castillero, título/renglón
   oficiales y ficha adjunta. Se distinguieron accesorios, sensores y mantenimiento
   de máquinas de anestesia que no equivalen al kit 43358.
6. Comparación con las hojas que consume Streamlit, registro manual `ct_rir_fichas`,
   cobertura/paginación de CLV, CLRIR y RIR1, cola de alertas y eventos de etapa.
   La ficha continúa en seguimiento. Cola CT RIR vacía y monitor sin error al corte.
7. Simulación sin envíos del recordatorio del último día: dos actos para el 01/10
   (Chepo y Vallarino) y uno para el 02/10 (Castillero), con consulta pública y sin errores.
   El monitor existente comprueba cada cinco minutos desde las 07:00, con deduplicación
   por acto y fecha. Requiere computadora encendida, orquestador activo y acceso a los servicios.

## Paginación y corridas

- CLV: corrida 30/09 11:00–11:11, cobertura completa. El portal detuvo el avance de
  páginas; el mecanismo de recuperación por API completó el listado. No se confundió
  un aviso de la interfaz con pérdida de registros cuando la recuperación sí terminó.
- CLRIR: 30/09 02:30–02:33, 419 enlaces en nueve páginas, sin fallos de detalle pendientes.
- RIR1: 29/09 18:40–18:50, cobertura completa con recuperación de la API.
- Las diferencias entre esos tamaños y la captura de esta auditoría son esperables:
  entre corridas cambian estados, se cierran CL y se publican nuevos actos.

## Protección añadida

El barrido de recuperación de CT RIR ahora incluye `cl_abiertas_419_sfd`,
`cl_prog_419_sfd` y `ap_419_sfd`. Antes esas tres fuentes no se examinaban en la vista
CT RIR ni en el barrido secundario del orquestador. Se exige igualmente coincidencia
con una ficha vigilada; Ley 419 por sí sola no clasifica un acto como 43358.
RS/SP y sus palabras clave permanecen sin cambios. Se conservan los eventos enviados,
el registro manual y los históricos; esta auditoría no envía correos ni vuelve a alertar
sobre oportunidades ya notificadas.

## Alcance de la conclusión

Es una verificación del corte y de los tres estados configurados, no una garantía
de todos los actos históricos o futuros. Se inspeccionaron todos los detalles del
listado y los anexos de los 11 candidatos, no los anexos de todos los 1,262 actos.
Una ficha mencionada únicamente en un adjunto de un acto con título y renglones ajenos
al producto sigue siendo una limitación de cobertura documental. No se asegura entrega
de correo en bandeja ni disponibilidad continua del portal por comprobar aceptación SMTP.

Evidencia local de solo lectura: `C:/Users/rodri/tmp/audit_ct43358_20260930/`.
Incluye listados originales, 1,262 detalles, comparación Sheets, anexos, OCR, simulaciones
y resultados del detector. Los originales del portal y las hojas operativas no se alteraron.
