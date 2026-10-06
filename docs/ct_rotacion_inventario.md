# Rotación e inventario de la ficha 43358

La vista aparece después de Estudio profundo y consulta las hojas nativas del
mismo archivo utilizado por el orquestador: `ct_rotacion_actos`,
`ct_rotacion_inventario` y `ct_rotacion_estado`. No necesita abrir Supabase ni
ejecutar el ranking maestro. La lectura se renueva cada 60 segundos.

`scripts/update_ct_rotation.py` reutiliza los endpoints y el cliente HTTP de
`db.db_api_updater` en el servidor. Captura CL y otros procedimientos, conserva
el histórico y sigue sus relaciones oficiales. Solo cuenta el renglón del kit
confirmado; no deduce la ficha desde códigos de clasificación. Si falla una
fuente, conserva los registros anteriores y comunica la incidencia. No envía
correos. `pc_config` programa el job `ct_rotacion_43358`.

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
