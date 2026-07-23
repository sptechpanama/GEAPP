@echo off
setlocal
set "ORQUESTADOR_SPREADSHEET_ID=1-2sgJPhSPzP65HLeGSvxDBtfNczhiDiZhdEbyy6lia0"
set "ORQUESTADOR_CONFIG_SHEET=pc_config"
set "ORQUESTADOR_STATE_SHEET=pc_state"
set "ORQUESTADOR_MANUAL_SHEET=pc_manual"
if not defined SUPABASE_DB_URL (
    for /f "tokens=2,*" %%A in ('reg query HKCU\Environment /v SUPABASE_DB_URL 2^>nul') do set "SUPABASE_DB_URL=%%B"
)
if not defined SUPABASE_DB_URL if not defined DATABASE_URL (
    echo [ERROR] Falta SUPABASE_DB_URL o DATABASE_URL en las variables de entorno.
    echo El orquestador no iniciara para evitar actualizaciones solo locales.
    pause
    exit /b 2
)
cd /d "C:\Users\rodri\scrapers_repo"
call .\.venv\Scripts\python.exe .\orquestador\main.py
pause
