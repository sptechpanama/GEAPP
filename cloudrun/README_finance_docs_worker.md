# Finance Docs Worker

Worker externo para crear borradores en `FinanzasDocs` desde carpetas Drive de `RIR` y `RS-SP`.

El flujo es:

1. Lista documentos nuevos en Drive.
2. Evita archivos ya procesados por `Drive file id` y hash.
3. Extrae texto:
   - PDF con texto: `pypdf`
   - Imagen: Google Cloud Vision OCR
   - PDF escaneado: render por pagina con PyMuPDF y OCR con Vision
4. Envia texto OCR/PDF a OpenAI.
5. Crea borrador en `FinanzasDocs`.
6. No registra en `Ingresos` ni `Gastos`; eso sigue ocurriendo solo al aprobar en Streamlit.

## Variables

Minimas:

```bash
SHEET_ID="..."
DRIVE_FINANCE_DOCS_FOLDER_ID_RIR="1GiaWnz2_qRAibxH0sQsqilUVMFKytjxo"
DRIVE_FINANCE_DOCS_FOLDER_ID_RS_SP="1s3BTZI5yu5KwCjIc7uinD_JUF3Nm_GGf"
OPENAI_API_KEY="..."
```

Opcionales:

```bash
OPENAI_FINANCE_DOC_MODEL="gpt-4o-mini"
FINANCE_DOCS_SCAN_LIMIT="50"
FINANCE_DOCS_MAX_PDF_PAGES="5"
FINANCE_DOCS_MAX_TEXT_CHARS="8000"
FINANCE_DOCS_OPENAI_MAX_TOKENS="800"
FINANCE_DOCS_WORKER_TOKEN="token-largo"
GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
```

Recomendado en Cloud Run:

- usar la identidad del servicio Cloud Run como credencial Google, o
- usar Secret Manager para `GOOGLE_SERVICE_ACCOUNT_JSON` y `OPENAI_API_KEY`

## APIs a habilitar

```bash
gcloud services enable run.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable vision.googleapis.com
gcloud services enable drive.googleapis.com
gcloud services enable sheets.googleapis.com
```

## Build de imagen

Crear repositorio Artifact Registry una vez:

```bash
gcloud artifacts repositories create geapp \
  --repository-format=docker \
  --location=us-central1
```

Build:

```bash
gcloud builds submit \
  --config cloudrun/finance_docs_worker.cloudbuild.yaml \
  --substitutions=_REGION=us-central1,_REPOSITORY=geapp,_IMAGE=finance-docs-worker
```

## Deploy Cloud Run

Ejemplo usando variables directas. Para produccion, cambia `OPENAI_API_KEY` y credenciales por Secret Manager.

```bash
PROJECT_ID="$(gcloud config get-value project)"

gcloud run deploy finance-docs-worker \
  --image us-central1-docker.pkg.dev/$PROJECT_ID/geapp/finance-docs-worker:latest \
  --region us-central1 \
  --no-allow-unauthenticated \
  --max-instances 1 \
  --timeout 300 \
  --set-env-vars SHEET_ID="TU_SHEET_ID" \
  --set-env-vars DRIVE_FINANCE_DOCS_FOLDER_ID_RIR="1GiaWnz2_qRAibxH0sQsqilUVMFKytjxo" \
  --set-env-vars DRIVE_FINANCE_DOCS_FOLDER_ID_RS_SP="1s3BTZI5yu5KwCjIc7uinD_JUF3Nm_GGf" \
  --set-env-vars OPENAI_FINANCE_DOC_MODEL="gpt-4o-mini" \
  --set-env-vars FINANCE_DOCS_SCAN_LIMIT="50" \
  --set-env-vars FINANCE_DOCS_MAX_PDF_PAGES="5"
```

## Scheduler horario

Usa Cloud Scheduler con OIDC hacia el endpoint `/run`.

```bash
WORKER_URL="$(gcloud run services describe finance-docs-worker --region us-central1 --format='value(status.url)')"
SCHEDULER_SA="finance-docs-scheduler@$PROJECT_ID.iam.gserviceaccount.com"

gcloud iam service-accounts create finance-docs-scheduler \
  --display-name="Finance Docs Scheduler"

gcloud run services add-iam-policy-binding finance-docs-worker \
  --region us-central1 \
  --member="serviceAccount:$SCHEDULER_SA" \
  --role="roles/run.invoker"

gcloud scheduler jobs create http finance-docs-worker-hourly \
  --location=us-central1 \
  --schedule="0 * * * *" \
  --uri="$WORKER_URL/run" \
  --http-method=POST \
  --oidc-service-account-email="$SCHEDULER_SA"
```

## Boton Verificar ahora

El boton actual de Streamlit sigue existiendo y procesa desde la app. El worker horario crea borradores sin abrir Streamlit.

## Permisos Drive

La identidad que use el worker debe poder leer las carpetas:

- RIR: `1GiaWnz2_qRAibxH0sQsqilUVMFKytjxo`
- RS-SP: `1s3BTZI5yu5KwCjIc7uinD_JUF3Nm_GGf`

Si usas la service account de Cloud Run, comparte esas carpetas con esa cuenta.
