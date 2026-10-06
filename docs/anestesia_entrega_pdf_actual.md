# Entrega PDF de Anestesia-Docs

## Carpeta de cotizaciones desde 2026-10-05

El flujo de un boton registra el numero oficial del acto en
`ANESTESIA_COTIZACIONES`; no añade un numero RIR al documento.
`RIR / Anestesia-Docs / Cotizaciones generadas` contiene exclusivamente
los PDF y Word finales. Los nombres indican destinatario y numero del acto.
El PDF debe tener una pagina de tamaño legal (8.5 × 14 pulgadas).
No se genera ZIP ni se copian certificados. Un acto nuevo no reemplaza otra
cotizacion; regenerar el mismo acto actualiza solamente sus dos archivos.

El publicador de cotizaciones comprueba ambos formatos y registra su
recuperacion en Sheets antes de reemplazar. Las carpetas temporales se retiran
cuando el proceso termina; una interrupcion conserva lo necesario para
recuperar antes del siguiente intento. Los archivos retirados van a la papelera.
El nuevo flujo valida automaticamente los datos del acto y de la cotizacion,
sin consultar la biblioteca ni bloquear por certificados faltantes o vencidos.
No declara una aprobacion independiente de ChatGPT. La segunda subcarpeta,
`Documentos y vigencias`, contiene once PDF originales con sus nombres del
ejemplo y el Sheets de control manual de vencimientos. Las solicitudes antiguas mantienen
el procedimiento que sigue, para no alterar registros y colas previas.

## Resultado de solicitudes del flujo anterior

Después de generar, revisar y revalidar el expediente, el orquestador publica
los PDF en `RIR / Anestesia-Docs / Entrega actual - PDF para presentar`.
El identificador de esta carpeta permanece igual entre solicitudes. Una
solicitud nueva reemplaza el conjunto completo, incluso si pertenece a otro
acto. La publicación no presenta la oferta en PanamáCompra.

La entrega estándar reproduce **12 archivos separados**, como las ofertas
RIR 1496274 (Ciudad de la Salud, excluyendo la cotización incorrecta) y 1498699
(Hospital Dr. Gustavo Nelson Collado):

1. Cotización membretada, generada para el acto y modelo seleccionados.
2. Paz y salvo DGI.
3. Paz y salvo CSS.
4. Certificado del Registro Público.
5. Certificado de oferentes.
6. Catálogo de oferentes (inscripción del producto).
7. Criterio técnico.
8. Catálogo del producto (`Ficha tecnica kit de anestesia.pdf` en el ejemplo).
9. Cédula del representante.
10. Aviso de operación.
11. Licencia de operaciones MINSA.
12. Método de destrucción.

No se agrupa ningún respaldo ni se alteran sus bytes, páginas o firmas.
Retorsión y declaración de calidad no forman parte de esas ofertas y se
eliminan de los requisitos base. El método de destrucción es el mismo PDF de
9 páginas anteriormente clasificado como `disposicion`; no es un documento
nuevo ni una declaración adicional. La licencia MINSA ya estaba importada
como `otro:Licencia de operaciones MINSA` y ahora se selecciona automáticamente.

La carpeta de entrega contiene únicamente PDF. El ZIP incluye exactamente esos
PDF; se guarda fuera de esa carpeta. El Word editable, manifiesto y revisión
quedan en las carpetas internas del expediente. Los PDF aprobados de cada
expediente tienen también una copia histórica independiente.

## Reemplazo y recuperación

La API de Drive no ofrece una transacción que cambie varios archivos de una vez.
El módulo se ejecuta desde la cola serial del orquestador existente:

1. Valida vigencias, revisión, acto y huellas de todos los documentos.
2. Prepara una copia íntegra de los PDF aprobados fuera de la entrega actual.
3. Guarda un registro de recuperación en Drive con los archivos anteriores.
4. Marca la carpeta `ACTUALIZANDO, no presentar` y archiva el conjunto anterior.
5. Copia el nuevo conjunto, relee todos los PDF y compara sus huellas y conteo.
6. Solo entonces marca la entrega como lista y actualiza el expediente.

Un fallo recuperable restaura los archivos anteriores. Si el proceso termina
abruptamente, la siguiente publicación recupera el estado anterior antes de
continuar. Si tampoco puede recuperarlo, la carpeta permanece marcada como no
disponible. Los archivos ajenos al módulo provocan un error y no se eliminan.
Una repetición de una publicación ya completada comprueba los PDF existentes y
no añade duplicados.

El botón principal **Ver archivos en Drive** abre directamente la carpeta
`Entrega <número del acto> - PDF para presentar - <versión>`, que contiene
únicamente los PDF finales del expediente seleccionado. Esta copia se conserva
aunque otra solicitud reemplace la entrega compartida. El ZIP se ofrece al lado.
Anexos, participaciones de referencia, Word y revisiones se consultan en un
desplegable aparte, cerrado por defecto. El botón de entrega queda deshabilitado
hasta completar la generación, revisión y validación de esa versión.
En expedientes sin copia individual, se verifica cada diez segundos qué acto y
versión ocupan la carpeta compartida antes de enlazarla; nunca se abre una entrega
de otro acto ni se usa una carpeta general como sustituto de los PDF finales.

## Verificación

- Regresión de generación, vigencias, almacenamiento, formularios y LP Generator.
- Igualdad de bytes, nombres documentales y separación de los 11 respaldos.
- Pruebas de reemplazo entre actos, reintento sin duplicados, reducción de 13 a 12,
  cambio de bytes, firma digital, documento adicional y archivo ajeno.
- Fallos de red, copia incompleta e interrupción abrupta del worker.
- Prueba real con la cuenta de servicio y una carpeta de Drive aislada: primera
  publicación de 12 PDF, reemplazo por otros 12 en el mismo ID y restauración
  completa después de una interrupción de red simulada. Se verificaron los bytes
  de los 12 archivos recuperados. La carpeta de prueba se envió a la papelera;
  los originales y expedientes de producción no se modificaron.

La prueba de Drive valida publicación y recuperación, no sustituye la revisión
del contenido de una oferta. Un certificado vencido, requisito faltante o
revisión pendiente sigue bloqueando la entrega final.

Referencia API: [Drive files y sus propiedades](https://developers.google.com/workspace/drive/api/reference/rest/v3/files).
