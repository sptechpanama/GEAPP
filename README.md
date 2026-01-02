# GEAPP / finapp

Aplicación financiera multipágina construida con Streamlit. Permite consultar métricas, sincronizar hojas de Google Sheets y realizar respaldos automáticos en Google Drive.

## Requisitos locales

1. Python 3.11 (se recomienda usar `py -3.11 -m venv .venv`).
2. Dependencias:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
3. Ejecutar la app:
   ```powershell
   streamlit run finapp/Inicio.py
   ```

## Configuración necesaria

### Secrets de Streamlit

Crear `finapp/.streamlit/secrets.toml` o configurar los *Secrets* en Streamlit Cloud con los bloques:

```toml
[google_service_account]
# Contenido completo del JSON de la cuenta de servicio

[app]
SHEET_ID = "..."
WS_ING = "Ingresos"
WS_GAS = "Gastos"
WS_TASKS = "Pendientes"
DRIVE_BACKUP_FOLDER_ID = "..."
BACKUP_PREFIX = "Finanzas Backup"
BACKUP_EVERY_DAYS = 3
BACKUP_KEEP_LAST = 15
DRIVE_PANAMACOMPRA_FILE_ID = "1TQYzsflXlE-5OwYKmTx0bTZ0rD9Ayivs"
DRIVE_TODAS_LAS_FICHAS_FILE_ID = "1AxQPm7koNkgV1txDdWpMA9SK2CfyY23Z"
DRIVE_OFERENTES_ACTIVOS_FILE_ID = "18thVq_8AqQ7BvnRd3V5sYFFNccXWWj7E"
```

### Variables de entorno (opcionales)

- `FINAPP_BASE_PATH`: ruta base para archivos auxiliares.
- `FINAPP_EXCEL_FICHAS`: ruta al Excel opcional de fichas.
- `FINAPP_DB_PATH`: ruta a la base SQLite `panamacompra.db`.
- `FINAPP_DRIVE_DB_FILE_ID`: ID del archivo en Drive usado para sincronizar `panamacompra.db`.
- `FINAPP_DRIVE_TODAS_FICHAS_ID`: ID alternativo para sincronizar `todas_las_fichas.xlsx` (opcional).
- `FINAPP_DRIVE_OFERENTES_ID`: ID alternativo para sincronizar `oferentes_activos.xlsx` (opcional).
- `FINAPP_SERVICE_ACCOUNT_FILE`: ruta a un JSON local de service account (se usa si no hay secrets).
- `FINAPP_DOMAIN_USER`: cuenta delegada para Drive (por defecto `soporte@sptechpanama.com`).

## Despliegue en Streamlit Cloud

1. Conectar el repositorio `sptechpanama/GEAPP` y seleccionar la rama `main`.
2. Elegir el script principal: `finapp/Inicio.py`.
3. Copiar a la sección *Secrets* el mismo contenido de `finapp/.streamlit/secrets.toml`.
4. Definir variables extra en *Advanced settings → Environment variables* si se usan (`FINAPP_DOMAIN_USER`, etc.).
5. Guardar y lanzar la aplicación. Streamlit instalará automáticamente las dependencias usando `requirements.txt` en la raíz.

## Notas adicionales

- El archivo `.gitignore` bloquea cualquier credencial local (`.streamlit/secrets.toml`, `*.json`).
- Si están configurados `DRIVE_PANAMACOMPRA_FILE_ID`, `DRIVE_TODAS_LAS_FICHAS_FILE_ID` u `DRIVE_OFERENTES_ACTIVOS_FILE_ID`, la app descargará automáticamente cada archivo desde Drive y usará las copias locales (`panamacompra.db`, `todas_las_fichas.xlsx`, `oferentes_activos.xlsx`) ubicadas en la raíz del repo.
- Para desarrollo colaborativo se puede usar el contenedor definido en `.devcontainer/`.
- Si aparecen advertencias de permisos al respaldar, verifica `DRIVE_BACKUP_FOLDER_ID` y la delegación de la cuenta de servicio.
