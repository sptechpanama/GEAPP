# Pipeline Estrategico

## Arquitectura

- **Supabase/PostgreSQL** es la fuente de verdad. Las tarjetas, controles,
  contactos, documentos, auditoría y la cola de sincronización se escriben en
  una sola transacción.
- **Google Sheets** es una replica legible y recuperable. El outbox permite
  reintentar una falla de Google sin perder ni duplicar cambios.
- **Google Drive** conserva los archivos. Supabase y Sheets guardan el ID, URL,
  nombre, tipo y responsable de cada adjunto.
- La identidad funcional de una tarjeta es `ficha + proveedor + marca`. Para
  una ficha desde cero se usa temporalmente el producto como identificador
  provisional hasta asignarle el numero definitivo.

## Configuracion

El modulo reutiliza `SUPABASE_DB_URL`, `SHEET_ID`, `DRIVE_TOPS_FOLDER_ID` y el
bloque `google_service_account` existentes. Opcionalmente pueden definirse:

```toml
[app]
PIPELINE_SHEET_ID = "ID_DE_LA_HOJA_NATIVA_GOOGLE"
DRIVE_PIPELINE_FOLDER_ID = "ID_DE_LA_CARPETA_EXACTA"
```

Si `PIPELINE_SHEET_ID` no existe, se usa `SHEET_ID`. Si
`DRIVE_PIPELINE_FOLDER_ID` no existe, se crea `Pipeline Estrategico` dentro de
`DRIVE_TOPS_FOLDER_ID`.

La replica crea cinco pestanas: `pipeline_cards`, `pipeline_checkpoints`,
`pipeline_contacts`, `pipeline_documents` y `pipeline_activity`.

## Reglas de negocio

- Fichas viejas y recién creadas usan 10 controles.
- Homologaciones, solicitudes y creación desde cero usan 13 controles y un
  primer control particular para su ruta.
- Un control no se puede completar si el anterior sigue pendiente.
- Reabrir un control ya superado requiere confirmación y reinicia todos los
  controles posteriores para evitar avances imposibles.
- Cambiar de categoría con avances también requiere confirmación y reinicia la
  lista usando la plantilla correcta.
- Duplicar crea una tarjeta independiente con los mismos datos, avance,
  contactos y referencias documentales. Los archivos de Drive se reutilizan
  mediante su enlace y no se copian físicamente.
- Eliminar retira la tarjeta del tablero mediante archivado lógico; no destruye
  su historial, contactos ni archivos y permite recuperación administrativa.
- La eliminación valida la versión de la tarjeta para no borrar silenciosamente
  cambios que otro usuario acaba de guardar.
- Todos los cambios registran usuario, fecha, accion y valor anterior/nuevo.

## Migracion desde Trello

La página acepta una exportación JSON real de Trello, muestra una vista previa
y luego importa de forma idempotente. Cargar dos veces el mismo JSON no duplica
tarjetas. Si varias tarjetas representan la misma combinación
`ficha + proveedor + marca`, se consolidan y se conserva el mayor avance
secuencial encontrado. Las plantillas sin proveedor o marca y las listas ajenas
al pipeline se omiten de forma explícita. El archivo `undefined - -.json`
recibido durante el desarrollo era
HTML de la interfaz de Trello y no contenia listas, tarjetas, checklists ni
campos personalizados; por eso no se uso para crear datos ficticios.
