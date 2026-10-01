# Entrega PDF de Anestesia-Docs

## Resultado de cada solicitud

Después de generar, revisar y revalidar el expediente, el orquestador publica
los PDF en `RIR / Anestesia-Docs / Entrega actual - PDF para presentar`.
El identificador de esta carpeta permanece igual entre solicitudes. Una
solicitud nueva reemplaza el conjunto completo, incluso si pertenece a otro
acto. La publicación no presenta la oferta en PanamáCompra.

La entrega estándar tiene **12 PDF**: cotización, DGI, CSS, Registro Público,
oferente e inscripción del producto agrupados, criterio técnico, catálogo,
cédula, aviso de operación, disposición final, retorsión y declaración de
calidad. El agrupado conserva todas las páginas y registra las huellas de
los dos originales en el manifiesto. La biblioteca permanece intacta.
Los dos documentos actuales de oferente e inscripción tienen una y cuatro
páginas respectivamente, sin campos de firma digital; pueden agruparse.

Si aparece una firma digital en cualquiera de esos dos originales, se mantienen
separados para no invalidarla. Tampoco se elimina un requisito adicional para
forzar el número 12: el conteo real y la excepción se muestran antes de publicar.

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

Streamlit comprueba cada diez segundos qué expediente y versión ocupan la
carpeta compartida. No muestra su botón de entrega como perteneciente a otro
acto ni como la versión nueva de un borrador aún pendiente. El usuario puede
abrir por separado los PDF históricos del expediente seleccionado.

## Verificación

- Regresión de generación, vigencias, almacenamiento, formularios y LP Generator.
- Comparación de texto y píxeles de las páginas agrupadas; conservación de originales.
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
