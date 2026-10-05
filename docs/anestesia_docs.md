# Anestesia-Docs: uso, comprobaciones y despliegue

## Dónde está

Generador de cotizaciones → **Anestesia-Docs**, inmediatamente después de LP Doc Generator.
La captura y conversión se ejecutan en la cola `pc_manual` del orquestador existente. Streamlit consulta el índice y permite editar los datos de la cotización; no ejecuta un navegador ni una generación prolongada.

## Flujo vigente desde 2026-10-05

La pantalla contiene Enlace, Mascarilla 4/K o 5/C, catalogo asociado, precio
unitario, ITBMS, termino del portal, plazo manual cuando corresponde,
Provincia y Hospital del acto y Almacen opcional. Las dos casillas documentan
la revision de adjuntos sobre plazo y destino; cambiar el enlace o los anexos
no transfiere esa confirmacion a otro acto.

Un solo boton, **Generar documentos**, guarda la configuracion y encola
`generate_quotation` en el mismo worker del orquestador. El worker captura
el portal y anexos, verifica el alcance 43358, cantidad, modelo, precio y
condiciones de entrega, y genera **solo la cotizacion membretada en Word/PDF**.
Revalida acto y anexos antes de publicar. Una captura incompleta, datos de
oferta invalidos o una modificacion bloquean la salida con su motivo.
No consulta ni copia los once certificados de la biblioteca; su ausencia o
vencimiento no bloquean la generacion de la cotizacion.

La validez es **120 dias calendario**, el pago **Credito** y garantia/esterilidad
**24 meses**. La fecha de la cotizacion corresponde a la publicacion oficial
del pliego. Precio y tratamiento ITBMS mantienen el calculo Decimal existente.
El membrete incorpora Engineering e info@rirmedical.com y conserva la razon
social S.EP. del ejemplo y la identidad registrada.

### Numeracion y carpetas

- El worker serial es el unico que reserva consecutivos: `RIR-000001`, etc.
- La hoja `ANESTESIA_COTIZACIONES` registra numero, consecutivo, acto, enlace,
  estado y carpeta; su JSON conserva configuracion, importes y enlace Word.
- Un acto distinto recibe otro consecutivo. Regenerar el mismo acto conserva
  su numero y carpeta; variantes del enlace no producen duplicados.
- `RIR / Anestesia-Docs / Cotizaciones / 000001 - <acto>` agrupa cada caso.
  Dentro, `Cotización membretada - PDF` contiene exclusivamente
  `01_Cotizacion.pdf`. El Word editable, fuentes y comprobaciones quedan aparte.
  No se genera un ZIP ni se copian otros documentos.
- El reemplazo de PDFs se limita a esa cotizacion, con respaldo y recuperacion;
  nunca reemplaza los archivos de otro acto. Los reintentos conservan el numero.
- La pantalla consulta el avance cada cinco segundos mientras corre y muestra
  enlaces a la cotizacion PDF, Word y todas las cotizaciones. El historial va cerrado.

La salida indica **Documentos generados**: comprobaciones automaticas,
no una aprobacion independiente de ChatGPT ni una presentacion en PanamaCompra.
Al regenerar un caso del formato anterior, los doce PDF anteriores se archivan
y la carpeta final se reemplaza por el unico PDF de cotizacion, conservando
su consecutivo. Los originales de biblioteca permanecen intactos.
Las acciones antiguas capture/generate/finalize y sus registros siguen siendo
compatibles para solicitudes ya existentes. La biblioteca de originales,
metadatos de verificacion e historial se conservan; el formulario compacto
no incluye los anteriores paneles de carga y auditoria.

## Vigencias del flujo histórico de expedientes completos

Estas reglas corresponden a los expedientes completos anteriores. No son
requisitos para el flujo vigente que genera exclusivamente la cotizacion.

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
- Sheets, libro usado por el orquestador: `ANESTESIA_COTIZACIONES`, `ANESTESIA_EXPEDIENTES`; se conservan `ANESTESIA_DOCUMENTOS` y `ANESTESIA_REVISIONES` para el historial.
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
