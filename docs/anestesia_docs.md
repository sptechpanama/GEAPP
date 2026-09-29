# Anestesia-Docs: uso, comprobaciones y despliegue

## Dónde está

Generador de cotizaciones → **Anestesia-Docs**, inmediatamente después de LP Doc Generator.
La captura y conversión de documentos se ejecutan en la cola `pc_manual` del orquestador existente. Streamlit consulta el índice y permite editar datos/cargar certificados; no ejecuta un navegador ni una generación prolongada.

## Secuencia

1. **Consultar acto y anexos**: introducir el enlace de PanamáCompra. Se verifica que el identificador y el número coincidan y se conservan los anexos originales. Los PDF escaneados se leen mediante OCR de Drive. Una captura incompleta bloquea la preparación.
2. **Biblioteca y vigencias**: cargar un PDF actualizado o seleccionar uno guardado para revisar sus metadatos. Registrar emisión, vencimiento, titular, ficha/modelo cubiertos y evidencia con página. Confirmar notaría, apostilla e idioma cuando correspondan. Guardar conserva el original y añade una versión; nunca renueva las fechas por haber subido el archivo.
3. **Datos y preparación**: elegir C (5) o K (4); introducir precio **unitario**, ITBMS exento/adicional/incluido, marca y modelo, entregas completas, lugar, garantía/esterilidad y datos del representante de la entidad para el pacto. Revisar requisitos adicionales.
4. **Comprobar requisitos y preparar borradores**: si falta un requisito, la tabla identifica exactamente qué actualizar. Si todos pasan, se generan cotización y pacto membretados y firmados por RIR en Word y PDF, más copias idénticas de los certificados originales. No se recrean certificados oficiales en Word ni se simula su firma notarial.
5. **Revisión de ChatGPT**: descargar el prompt del expediente y compartir los archivos de la carpeta con el chat que utilizarás. Elegir el modelo disponible en tu cuenta. El programa no inicia ni controla automáticamente tu chat personal. La revisión debe cubrir todos los documentos, páginas y requisitos y devolver `revision_anestesia.json`.
6. **Revalidar y publicar**: adjuntar el JSON, revisar sus resultados y confirmar. Antes de publicar se comprueban otra vez los certificados, las versiones, los anexos del acto, el cierre y la integridad de cada archivo. Se crea una carpeta de entrega con ZIP e índice. No se presenta una oferta automáticamente.

## Vigencias y alcance

- DGI, CSS/no cotizante, oferente, CT, RS y cédula requieren una fecha de vencimiento verificable. Se comprueba la fecha de presentación, no solo el día de generación.
- Registro Público: la antigüedad máxima se toma del **pliego concreto**. En los dos actos revisados se exige no mayor de **un año**. No se fija universalmente en seis meses. Si no puede extraerse, exige confirmar la regla con archivo y página.
- Un catálogo o aviso sin vencimiento impreso requiere confirmar esa condición; no se inventa una fecha de emisión o expiración.
- CT, catálogo e inscripción del producto deben acreditar el mismo modelo. El CT inicial MINSA-MQ-2850-01-26 identifica **LB4330K** y vence el **26/01/2031**. No acredita automáticamente el producto del catálogo C.
- Las declaraciones notariales y los documentos extranjeros mantienen los requisitos propios del acto. La firma insertada de RIR no sustituye notaría, apostilla, traducción ni firma de la entidad.
- El número final de documentos depende de los requisitos. La plantilla de control base incluye 12 originales, dos documentos propios y los requisitos adicionales aplicables; contar archivos no demuestra cumplimiento.
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
- Biblioteca inicial de 10 PDF conservados con SHA-256 y metadatos pendientes de confirmación. DGI y CSS localizados están vencidos; deben reemplazarse. CT K y oferente conservan las fechas impresas, sin extender su vigencia por haberlos importado.
- En la captura analizada el Top permite revisar 2 investigaciones con contacto trazable y presenta 36 pares acto/ficha sin investigación. Se detectaron 22 seguimientos escritos en observaciones con `actualizado_en` anterior: la corrección de ese contenido debe publicarla ChatGPT, después de revisarlo realmente.
- No hubo navegador conectado para comprobar visualmente la sesión autenticada de Streamlit Cloud. La comprobación de interfaz se realizó con AppTest; el despliegue se publica en la rama `main` consumida por la app.
