# Rotación e inventario de la ficha 43358

La vista aparece después de Estudio profundo y consulta las hojas nativas del
mismo archivo utilizado por el orquestador: `ct_rotacion_actos`,
`ct_rotacion_inventario` y `ct_rotacion_estado`. No necesita abrir Supabase ni
ejecutar el ranking maestro. La lectura se renueva cada 60 segundos.

`scripts/update_ct_rotation.py` lee las bases locales ya extraídas por los
procesos normales: `panamacompra.db` e `inteligencia_proveedores.db`. No consulta
PanamáCompra, no descarga documentos y no ejecuta scrapers adicionales.
`orquestador/database_pipeline.py` publica el histórico después de construir la
analítica médica dentro de la actualización habitual de la base. No tiene un
horario independiente; el antiguo job `ct_rotacion_43358` está deshabilitado en
`pc_config`. `--dry-run` permite comprobar el resultado sin modificar Sheets.

La capa analítica sirve únicamente como índice de candidatos; las cantidades se
toman del renglón con ficha explícita, o del kit único de un acto previamente
verificado. No se deduce la ficha desde códigos de clasificación. Se conservan
las relaciones y requisiciones auditadas, además de las relaciones guardadas por
el seguimiento habitual de CL. Si falta una cantidad, una fuente no puede leerse
o un acto desaparece del índice, no se elimina el histórico confirmado. La
publicación no modifica el inventario ni envía correos.

Las convocatorias CL y sus actos derivados se cuentan una vez. Las
republicaciones documentadas de una misma requisición se agrupan y las
cancelaciones sin continuación se excluyen de la demanda. Los promedios
mensuales usan enero de 2025 hasta la fecha actual, incluyendo los meses sin
solicitudes y la fracción del mes actual. La frecuencia de cada hospital usa
todo su historial depurado. La fecha estimada no es una compra anunciada y no
se desplaza artificialmente al futuro cuando ya pasó.

El inventario real se registra manualmente con su fecha y queda en Sheets.
La proyección consume el 60% de la demanda mensual, suponiendo cantidades
representativas de los actos ganados. Usa un mes de fabricación en China,
un mes de tránsito y 6,000 kits por embarque. El stock proyectado no sustituye
un conteo físico ni descuenta ventas reales no registradas.

La inicialización admite `--seed-audit DIRECTORIO` con `analysis.json` y
`statistics_extra.json` de una auditoría verificada. El seed no incluye ni
sobrescribe el inventario. La ejecución ordinaria no depende de esos archivos.
