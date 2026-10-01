# Anestesia-Docs: uso, comprobaciones y despliegue

## Dónde está

Generador de cotizaciones → **Anestesia-Docs**, inmediatamente después de LP Doc Generator.
La captura y conversión de documentos se ejecutan en la cola `pc_manual` del orquestador existente. Streamlit consulta el índice y permite editar datos/cargar certificados; no ejecuta un navegador ni una generación prolongada.

## Secuencia

Al abrir la pestaña aparece **Documentos actuales y vigencias**: una fila por documento
aplicable al catálogo K o C. Se muestran la última versión, emisión, vencimiento,
enlace al original y motivo del estado. El historial completo queda en un desplegable
de Biblioteca y vigencias. Los estados diferencian Vigente documentalmente, Vence hoy,
Vence pronto (7 días), Vencido, Falta, Pendiente de verificar y Según pliego.
El panel consulta de nuevo cada 60 segundos mientras está abierto, usando la fecha de Panamá.
Un corte de red se informa como tal; no se interpreta como biblioteca vacía.

Para reemplazar un certificado: **Biblioteca y vigencias → seleccionar documento o Nuevo PDF →
adjuntar original actualizado → registrar datos y evidencia → Guardar nueva versión y verificación**.
DGI/CSS se contrastan contra el emisor, titular y fechas impresas del PDF; si es escaneado,
se intenta OCR en Drive. La carga queda pendiente si no puede comprobarse o no coincide.
Marcar la casilla de revisión no anula esas comprobaciones. La huella vincula la validación
al archivo y sus metadatos. En los otros documentos se conserva la revisión registrada
de alcance y formalidades: el programa no sustituye al emisor ni certifica autenticidad.

Antes de crear siquiera la cotización, el worker comprueba también los PDF reales y sus
huellas, además de las reglas del expediente. Actualizar una fecha en la interfaz o
cargar una nueva versión incompleta no permite utilizar silenciosamente una versión anterior.
La vigencia debe cubrir la presentación del acto, aunque el documento todavía sea válido hoy.

1. **Consultar acto y anexos**: introducir el enlace de PanamáCompra. Se verifica que el identificador y el número coincidan y se conservan los anexos originales. Los PDF escaneados se leen mediante OCR de Drive. Una captura incompleta bloquea la preparación.
2. **Biblioteca y vigencias**: cargar un PDF actualizado o seleccionar uno guardado para revisar sus metadatos. Registrar emisión, vencimiento, titular, ficha/modelo cubiertos y evidencia con página. Confirmar notaría, apostilla e idioma cuando correspondan. Guardar conserva el original y añade una versión; nunca renueva las fechas por haber subido el archivo.
3. **Datos y preparación**: elegir C (5) o K (4); introducir precio **unitario** e ITBMS exento/adicional/incluido. La marca/modelo se obtiene de esa selección, la garantía habitual es de 24 meses y el lugar se extrae del acto. Confirmar calendario completo y anexos.
4. **Comprobar requisitos y preparar borradores**: si falta un requisito, la tabla identifica qué actualizar. Si todos pasan, se genera la cotización membretada y firmada por RIR en Word y PDF, más copias idénticas de los 11 respaldos de las ofertas de ejemplo. No se genera un pacto bilateral en esta etapa.
5. **Revisión de ChatGPT**: descargar el prompt del expediente y compartir los archivos de la carpeta con el chat que utilizarás. Elegir el modelo disponible en tu cuenta. El programa no inicia ni controla automáticamente tu chat personal. La revisión debe cubrir todos los documentos, páginas y requisitos y devolver `revision_anestesia.json`.
6. **Revalidar y publicar**: adjuntar el JSON, revisar sus resultados y confirmar. Antes de publicar se comprueban otra vez los certificados, las versiones, los anexos del acto, el cierre y la integridad de cada archivo. **Ver archivos en Drive** abre la carpeta de los 12 PDF del caso; ZIP, índice y referencias se guardan aparte. No se presenta una oferta automáticamente.

## Vigencias y alcance

- DGI, CSS/no cotizante, oferente, CT, licencia MINSA y cédula requieren una fecha de vencimiento verificable. Se comprueba la fecha de presentación, no solo el día de generación.
- Registro Público: la antigüedad máxima se toma del **pliego concreto**. En los dos actos revisados se exige no mayor de **un año**. No se fija universalmente en seis meses. Si no puede extraerse, exige confirmar la regla con archivo y página.
- Un catálogo o aviso sin vencimiento impreso requiere confirmar esa condición; no se inventa una fecha de emisión o expiración.
- CT, catálogo e inscripción deben acreditar el modelo ofrecido. La copia local inicial era distinta e incompleta para esta revisión. El CT completo recuperado de las participaciones, **C.S.S.-MQ-8492-09-26/C**, vence el **15/09/2031**: su segunda página incluye **LB4330K (mascarilla talla 4)** y **LB4330C (talla 5)**. El catálogo comercial, página 4, y el registro de oferentes, páginas 2-3, también incluyen ambos. La cotización identifica cuál se ofrece; que el CT cubra dos modelos no significa ofertar ambos.
- Las declaraciones notariales y los documentos extranjeros mantienen los requisitos propios del acto. La firma insertada de RIR no sustituye notaría, apostilla, traducción ni firma de la entidad.
- La plantilla base reproduce la lista verificada de las ofertas de ejemplo: cotización nueva y 11 respaldos separados. Retorsión y calidad no están en esa lista; licencia MINSA y método de destrucción sí. Véase [cotejo de adjuntos](anestesia_cotejo_adjuntos_2026-09-30.md).
- Los metadatos cargados por el usuario deben comprobarse contra el original. El sistema valida reglas y fechas; el OCR no certifica autenticidad ni una aprobación técnica. La revisión debe consultar al emisor cuando un documento exige validación de autenticidad.

## Comprobación con los actos proporcionados

| Acto | Unidad de compra | Cantidad | Entregas identificadas | Situación en la revisión inicial |
|---|---|---:|---|---|
| 2026-1-10-01-08-CL-051191 | CSS – Ciudad de la Salud | 900 | 300 a 30 días, 300 a 45 y 300 a 60; verificar punto de inicio en el anexo | Cerrado; ejemplo histórico |
| 2026-1-10-01-06-CL-051243 | CSS – Hospital Dr. Gustavo Nelson Collado R. | 1,500 | Entrega total a 30 días calendario | Cerrado; ejemplo histórico |

No se publicó una oferta real para estos ejemplos ni se inventó un precio de participación. Los archivos de prueba visual están en una carpeta identificada **PRUEBAS DE FORMATO - NO PRESENTAR**.

## Persistencia y configuración

- Drive: carpeta `RIR/Anestesia-Docs`, biblioteca, fuentes oficiales, borradores, originales y entregas.
- Sheets, libro usado por el orquestador: `ANESTESIA_DOCUMENTOS`, `ANESTESIA_EXPEDIENTES`, `ANESTESIA_REVISIONES`.
- Se reutilizan las credenciales existentes. No se añaden claves, contraseñas ni correos al código.
- Worker: `scrapers_repo/orquestador/anestesia_docs_worker.py`; job manual `anestesia_docs`. No tiene una ejecución diaria automática y no modifica el horario de otros trabajos.
- Localmente, `ANESTESIA_GEAPP_PATH` puede señalar la copia desplegada de GEAPP. Alternativa local ignorada por Git: `orquestador/anestesia_docs.local.json` con `{"geapp_path":"RUTA_ABSOLUTA_GEAPP"}`.
- Opcionales en secretos de Streamlit `[app]`: `ANESTESIA_PYTHON`, `ANESTESIA_WORKER`, `PC_MANUAL_SHEET_ID`, `DRIVE_COTIZACIONES_FOLDER_ID`. Los valores predeterminados reutilizan las rutas y el libro existentes.
- Si `PC_MANUAL_SHEET_ID` apunta al antiguo XLSX de fichas, se reconoce el error específico de Office y se utiliza el libro nativo existente del orquestador, después de comprobar que contiene `pc_config` y `pc_manual`. No se convierte ni modifica el XLSX y no se crea otra cola. Errores de permisos o cortes de red se muestran sin cambiar silenciosamente de biblioteca. La sesión conserva el identificador resuelto y se invalida si cambia la configuración.
- El servidor debe estar encendido con el orquestador activo. Si una solicitud queda **En cola**, comprobarlo antes de volver a crearla; el historial permanece en Sheets.

## Top RIR y pacto

El Top permite investigar proveedores con correo y cotización trazable aunque no tengan enlace público; se identifican como **Para cotizar o confirmar**. Mantiene exclusiones de actos vencidos, fichas con requisitos, mezcla global y fuentes sin verificar. Un bloque separado lista actos/fichas actuales sin investigación, sin inventar renglones ni proveedores.

Si ChatGPT escribió seguimientos nuevos en las observaciones pero dejó `actualizado_en` antiguo, se informa el desfase. La app no cambia esa fecha para aparentar una investigación nueva. Usar `prompt_rir_reparar_publicacion.md` y luego `prompt_rir_top10_diario.md`.

LP Doc Generator incorpora **Cargo del representante de la entidad (Pacto)**. Cambia tanto su presentación como su etiqueta de firma en RS, RIR y SP; preserva la identidad del oferente.

## Validación de esta entrega

- 193 pruebas automatizadas: cantidades e ITBMS, precisión del precio unitario, caducidad, antigüedad por pliego, cobertura CT/modelo, notaría, apostilla, archivos alterados, cambios del acto, revisión incompleta, conservación de originales e idempotencia de publicación. Incluyen interacción real con controles Streamlit mediante AppTest, con servicios externos simulados.
- Captura real de los dos actos solicitados: 3 y 5 anexos PDF, lectura OCR donde correspondía, ficha explícita 43358 y antigüedad del Registro Público de 12 meses en ambos.
- Ejecución real desde `pc_manual`: solicitud recibida y completada por el orquestador en aproximadamente 47 segundos, con enlaces de resultado. Sin envío de correos ni presentación de ofertas.
- Conversión real mediante Drive y revisión visual del PDF de cotización y pacto; una página cada uno en el caso de prueba. Los originales oficiales no se recrearon.
- La biblioteca inicial conservó 10 PDF con SHA-256. La revisión posterior de participaciones recupera los certificados actuales y conserva aquellas versiones como historial; la importación no cambia su vigencia.
- En la captura analizada el Top permite revisar 2 investigaciones con contacto trazable y presenta 36 pares acto/ficha sin investigación. Se detectaron 22 seguimientos escritos en observaciones con `actualizado_en` anterior: la corrección de ese contenido debe publicarla ChatGPT, después de revisarlo realmente.
- No hubo navegador conectado para comprobar visualmente la sesión autenticada de Streamlit Cloud. La comprobación de interfaz se realizó con AppTest; el despliegue se publica en la rama `main` consumida por la app.

## Recuperación de participaciones y corrección del 29/09/2026

- Se consultaron exclusivamente las participaciones de RIR, cotejando nombre y RUC con la oferta pública. El cuadro se obtiene desde `/documentos-actos-publico/cuadroPropuesta/2/procesoVistaCuadroPropuesta/{flow}` y el detalle desde `/procesos-configuracion/pagina-componentes-publico/2/procesoOfertaDetallePropuestaPublico/{flow}/{oferta}`. Los adjuntos se descargan de las rutas públicas que devuelve el detalle.
- Ciudad de la Salud contiene **13 adjuntos** y Collado **12**: son **14 contenidos distintos** entre las dos participaciones. Se conservan todos los PDF originales por acto y se incorporan los documentos útiles a la biblioteca, sin duplicar su importación al repetirla. Se comprueba SHA-256 tras leer de nuevo cada documento de biblioteca desde Drive.
- Ciudad de la Salud: la cotización inicial a USD 24.50 se archiva como **NO REUTILIZAR - COTIZACIÓN INCORRECTA**. La posterior, a USD 22.50, total USD 20,250 y entregas 300/300/300, coincide con la oferta electrónica y ofrece LB4330K. Collado ofrece LB4330K a USD 18.50, total USD 27,750, entrega a 30 días. Son antecedentes históricos, no nuevas ofertas autorizadas.
- CSS: emitido **23/09/2026**, vence **30/09/2026**. DGI: emitido **22/09/2026**, vence **20/10/2026**. Registro Público: emitido **16/09/2026**, antigüedad contrastada con cada pliego. Oferente: vence **15/04/2027**, no en la fecha de la licencia de operaciones (**15/10/2029**).
- El método de destrucción conserva sus **9 páginas**, carta, apostilla y traducción. La carta del 03/12/2025 declara tres años de vigencia. Queda pendiente comprobar la suficiencia de su descripción genérica para cada pliego; tener un PDF apostillado no equivale a aprobar el requisito específico.
- Los metadatos verificados significan lectura de titular, fechas y alcance en el documento. No sustituyen comprobación de autenticidad ante los emisores, ni las declaraciones específicas y demás requisitos de una futura oferta.
- Cada expediente muestra el enlace a la cotización de referencia y al archivo de su participación. No se cambian los precios, firmas autorizadas ni configuración de una nueva oferta por importar antecedentes.
- Se bloquea seleccionar C con modelo LB4330K, o K con LB4330C, aunque el CT contenga ambos. Se cubren con pruebas la recuperación de Office, conservación de cola/biblioteca, fallos de acceso, sesiones anteriores y ambas variantes documentales.
- Validación de la corrección: **207 pruebas aprobadas**, compilación de la página y servicios, y AppTest conectado a los datos reales usando precisamente el identificador XLSX que fallaba. Abrió los expedientes, los antecedentes y las **21 versiones** de biblioteca (10 anteriores + 11 importadas/revisadas). La segunda importación añadió **0 registros** y reutilizó los mismos archivos. Se comprobó con la biblioteca real que K y C seleccionan el CT completo y que CSS se bloquea para una fecha de presentación posterior al 30/09/2026.
