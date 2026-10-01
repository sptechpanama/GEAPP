# Registro Público en Anestesia-Docs: ficha 43358

## Resultado y regla aplicada

La antigüedad máxima más exigente encontrada en las cláusulas identificadas de
esta revisión es **12 meses desde la emisión**. No se encontró una cláusula
verificable de seis meses para el Registro Público de estos actos. Esta es una
regla operativa sustentada en los anexos revisados, no una declaración de que
todos los actos futuros deban aceptar un año.

Referencia principal: **2026-1-10-01-08-CL-051191, Ciudad de la Salud**, ficha
43358, documento *MODELO CONTRATACION MENOR O COMPRA MENOR AGIL-INSUMO - V 2.pdf*,
página 2: «vigencia no mayor de un (1) año».

- [Acto oficial](https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-08-CL-051191/0nM6ICc0JCLwkjN2QDMxojIpJye).
- [Anexo oficial, página 2](https://apisv3.panamacompra.gob.pa/procesos-contratacion-archivos/v2/download-file-51AEBAF2-82B3-F111-80EB-0017A477FC64-ZA-101-1AZ-12500129-ZA-101-1AZ-12500129).
- PDF descargado de nuevo el 30/09/2026 y página 2 revisada visualmente.
- SHA-256 del PDF: `ba88cf1d0d5181c809bfa7564aaa155906dfd8c06c549fccc306f13c8b8ce3d9`.

La misma condición de un año figura en los anexos de Vallarino
**2026-1-10-01-08-CL-051598** (KIT ANESTESIA.pdf, página 1), Chepo
**2026-1-10-01-08-CL-051614** (requerimientos, página 2) y Gustavo Nelson Collado
**2026-1-10-01-06-CL-051243** (ESPECIFICACIONES TECNICAS 1001142091001.pdf,
sección Registro Público). Estos anexos también fueron contrastados con sus
textos completos/OCR.

## Alcance del contraste histórico

Consulta SQLite de solo lectura de las 52 filas cuyo campo `ficha_detectada`
contiene 43358. Se capturó el detalle oficial de las 52 y se siguieron sus
relaciones a 49 procesos originales distintos. En total: **101 expedientes,
518 referencias a archivos adjuntos y OCR local de 899 páginas escaneadas**.
Los 101 detalles fueron accesibles. En 99 se encontró además el número 43358
en la respuesta oficial o sus anexos; las dos asociaciones sin esa
confirmación no se utilizaron para fijar el plazo.

En **35 expedientes** se extrajo una cláusula de antigüedad máxima vinculada al
Registro Público, en todos los casos de 12 meses. Se examinaron separadamente
los párrafos con otros plazos y los textos OCR no reconocidos por el parser.
En muchos expedientes derivados solo se publican adjudicación, orden de
compra y recepción: la ausencia de cláusula ahí no equivale a ausencia del
requisito. Los cuatro ejemplos recientes anteriores complementan la revisión.

Limitaciones: este contraste parte del histórico local disponible y de sus
procesos relacionados; no certifica la totalidad histórica de PanamáCompra.
El OCR ayuda a localizar cláusulas, pero una página vacía, incompleta o ilegible
no constituye prueba de que no haya un requisito adicional. No se utilizaron
fechas de poderes, paz y salvos, garantías del producto o retorsiones como
antigüedad del Registro Público.

Evidencia detallada local de solo lectura:
`C:\Users\rodri\tmp\registry_history_20260930\final_research.json`, con actos,
archivos, páginas, URLs y cláusulas; PDFs y textos en `files` y
`originals/files`. No se alteró la base de actos, Drive ni Sheets para realizar
la investigación.

## Funcionamiento implementado

- Anestesia-Docs calcula el vencimiento para presentación como **emisión + 12
  meses calendario**. Si hay vencimiento impreso anterior, prevalece este.
- Un plazo más corto detectado en el acto actual prevalece. Se relee también
  el texto de anexos guardados para aprovechar capturas hechas con el parser
  anterior. Un valor manual antiguo no puede extender el plazo del acto.
- Biblioteca, historial y validación de generación usan el mismo cálculo.
  Aparecen vigente, vence pronto, vence hoy o vencido según la fecha de control.
- Se conserva la actualización visual cada 60 segundos y se revalida al
  generar y al finalizar. Subir de nuevo el mismo certificado no renueva su edad.
- El certificado actual de RIR, con emisión registrada el **16/09/2026**, muestra
  **16/09/2027** como límite para presentación bajo la regla de 12 meses.
  Esto se comprobó contra Drive/Sheets con permisos de solo lectura mediante
  la interfaz real de Streamlit en AppTest; no es una consulta de autenticidad
  al Registro Público ni una inspección de la sesión de Streamlit Cloud.
- Se retiraron del formulario las cajas de antigüedad manual, archivo/página
  y otros documentos. La procedencia de la regla queda guardada internamente.
  Requisitos adicionales ya guardados permanecen visibles como texto y se
  siguen validando; no se borran documentos ni datos históricos.
- Se mantienen los controles de emisión, titular, revisión del original,
  hash, modelo/catálogo y requisitos específicos. La biblioteca no altera las
  fechas impresas ni los PDF originales.

## Pruebas

Casos de año bisiesto, fin de mes, vencimiento en el día, plazo impreso anterior,
datos no verificados, falta de emisión, regla más estricta, configuración antigua,
OCR con números entre paréntesis y plazos que pertenecen a otros documentos.
Pruebas del formulario real para ausencia de las cajas y conservación de
requisitos previos, más regresión de cola/almacenamiento, generación y LP Generator.
Resultado: **220 pruebas aprobadas**. Comprobación adicional con la biblioteca
real en modo de solo lectura y compilación de módulos correcta.
La publicación del código se realiza en GEAPP/main.
