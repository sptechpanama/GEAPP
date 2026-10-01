# Cotejo documental de las ofertas RIR

## Fuentes verificadas

- [Ciudad de la Salud, CL-051191](https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-08-CL-051191/0nM6ICc0JCLwkjN2QDMxojIpJye): flujo 1046690, oferta RIR 1496274. Contiene 13 adjuntos; el 12613033 es la cotización incorrecta indicada por el usuario y no se reutiliza. La cotización corregida es 12613035.
- [Hospital Dr. Gustavo Nelson Collado, CL-051243](https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-06-CL-051243/0nM6ICc0JCLwUDN3QDMxojIpJye): flujo 1047450, oferta RIR 1498699. Contiene 12 adjuntos.

Se consultó nuevamente el endpoint público `procesoOfertaDetallePropuestaPublico/{flujo}/{oferta}` para ambas ofertas. Los identificadores coincidieron con la captura previa. Se descargaron de nuevo los 12 PDF de Collado y se contrastó cada SHA-256 con el original archivado. No se revisaron ofertas de otros proveedores como plantilla.

## Correspondencia exacta

| Documento | Adjunto Ciudad de la Salud | Adjunto Collado | Páginas del respaldo |
|---|---:|---:|---:|
| Cotización membretada (generar para cada caso) | 12613035 | 12635950 | 1 en los ejemplos |
| Aviso de operación | 12613025 | 12635938 | 1 |
| Cédula | 12613026 | 12635939 | 1 |
| Criterio técnico | 12613037 | 12635940 | 2 |
| Catálogo de oferentes | 12613036 | 12635941 | 4 |
| Certificado de oferentes | 12613034 | 12635942 | 1 |
| Paz y salvo CSS | 12613031 | 12635943 | 1 |
| Paz y salvo DGI | 12613032 | 12635944 | 1 |
| Certificado del Registro Público | 12613027 | 12635945 | 1 |
| Método de destrucción | 12613030 | 12635946 | 9 |
| Licencia de operaciones MINSA | 12613029 | 12635947 | 1 |
| Catálogo del producto (`Ficha tecnica kit de anestesia.pdf`) | 12613028 | 12635948 | 5 |

Retorsión y declaración de calidad no aparecen en estas ofertas y salen del control base. El método de destrucción fue llamado erróneamente «Disposición final del fabricante» en la interfaz: se usa el nombre del PDF aportado. La licencia MINSA era un registro adicional ya importado y ahora pertenece al control base.

Oferente e inscripción se conservan separados. Los 11 respaldos se copian con sus bytes y páginas completos; no se agrupan para alcanzar un número artificial. La cotización se genera con el acto, cantidades, precio, ITBMS y modelo seleccionados, sin copiar precios ni plazos de los ejemplos.

## Vigencias y alcance de la revisión

La licencia MINSA imprime emisión 16/10/2024, actualización 25/11/2025 y vencimiento 15/10/2029. El método de destrucción, página 3, está fechado 03/12/2025 y declara tres años de vigencia; se conserva el 03/12/2028 registrado. Se inspeccionaron sus nueve páginas, incluida apostilla y traducciones. Se actualiza la clasificación y evidencia del índice sin alterar ni borrar el PDF original.

La comprobación de PDF, identidad y fechas no equivale a verificación de autenticidad en línea ni garantiza la admisión de una oferta futura. Los controles de vencimiento, acto y revisión final continúan vigentes. El paz y salvo CSS del ejemplo vence 30/09/2026 y no cubre una presentación el 01/10/2026: debe reemplazarse por el certificado vigente.
