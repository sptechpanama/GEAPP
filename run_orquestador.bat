@echo off
setlocal
set "ORQUESTADOR_SPREADSHEET_ID=1-2sgJPhSPzP65HLeGSvxDBtfNczhiDiZhdEbyy6lia0"
set "ORQUESTADOR_CONFIG_SHEET=pc_config"
set "ORQUESTADOR_STATE_SHEET=pc_state"
set "ORQUESTADOR_MANUAL_SHEET=pc_manual"
cd /d "C:\Users\rodri\scrapers_repo"
call .\.venv\Scripts\python.exe .\orquestador\main.py
pause
