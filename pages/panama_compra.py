# pages/visualizador.py
import streamlit as st
import pandas as pd
from sheets import get_client

st.set_page_config(page_title="Visualizador de Actos", layout="wide")
st.title("📋 Visualizador cómodo de Actos (CL / RIR)")

# ---- Config ----
SHEET_ID = "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo"


TABS = [
    "ap_con_ct", "ap_sin_ficha",
    "cl_prog_sin_ficha", "cl_prog_sin_requisitos", "cl_prog_con_ct",
    "cl_abiertas", "cl_abiertas_rir_sin_requisitos", "cl_abiertas_rir_con_ct",
    "cl_prioritarios",
]

def get_gc():
    # Reutiliza las credenciales centralizadas en sheets.get_client()
    client, _ = get_client()
    return client

# --- reemplaza tu load_df y añade el helper _make_unique ---

@st.cache_data(ttl=300)
def load_df(sheet_name: str) -> pd.DataFrame:
    sh = get_gc().open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_name)

    # 1) Intento normal: usar fila 1 como encabezados, pero saneados/únicos
    raw_headers = ws.row_values(1)

    def _make_unique(headers):
        out, seen = [], {}
        for i, h in enumerate(headers):
            h = (h or "").strip() or f"col_{i+1}"   # rellena vacíos
            if h in seen:
                seen[h] += 1
                h = f"{h}_{seen[h]}"                # evita duplicados
            else:
                seen[h] = 0
            out.append(h)
        return out

    # Si la fila 1 está vacía, buscamos la primera fila "buena" (hasta 10 filas)
    if not any(c.strip() for c in raw_headers):
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()

        header_row_idx = None
        for r in range(min(10, len(values))):
            non_empty = sum(1 for c in values[r] if c.strip())
            if non_empty >= 3:                      # regla simple: al menos 3 celdas con datos
                header_row_idx = r
                break

        if header_row_idx is None:
            return pd.DataFrame()

        headers = _make_unique(values[header_row_idx])
        width = len(headers)
        data = [row[:width] + [""]*(width - len(row)) for row in values[header_row_idx+1:]]
        df = pd.DataFrame(data, columns=headers)
    else:
        headers = _make_unique(raw_headers)
        # 2) Forzar a gspread a usar estos headers únicos
        records = ws.get_all_records(expected_headers=headers)
        df = pd.DataFrame(records)

    # 3) Limpieza mínima: quitar filas totalmente vacías
    df = df.replace("", pd.NA).dropna(how="all")
    return df


def render_df(df: pd.DataFrame, sheet_name: str):
    keyp = f"{sheet_name}_"

    # Detectar columna de fecha (cualquiera que empiece por "Fecha")
    date_col = next((c for c in df.columns if c.lower().startswith("fecha")), None)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Detectar columnas de monto/precio
    money_cols = [c for c in df.columns if any(x in c.lower() for x in ["precio", "monto", "estimado", "referencia"])]

    with st.expander("🔎 Filtros", expanded=True):
        if "Entidad" in df.columns:
            opciones = sorted([e for e in df["Entidad"].dropna().unique()])
            sel = st.multiselect("Entidad", opciones, key=keyp+"ent")
            if sel: df = df[df["Entidad"].isin(sel)]

        if "Estado" in df.columns:
            opciones = sorted([e for e in df["Estado"].dropna().unique()])
            sel = st.multiselect("Estado", opciones, key=keyp+"estado")
            if sel: df = df[df["Estado"].isin(sel)]

        if date_col and df[date_col].notna().any():
            mind, maxd = df[date_col].min(), df[date_col].max()
            r = st.date_input("Rango de fechas", value=(mind.date(), maxd.date()), key=keyp+"fecha")
            if isinstance(r, tuple) and len(r) == 2:
                ini, fin = pd.to_datetime(r[0]), pd.to_datetime(r[1])
                df = df[(df[date_col] >= ini) & (df[date_col] <= fin)]

        if money_cols:
            colm = money_cols[0]
            v = pd.to_numeric(df[colm], errors="coerce")
            if v.notna().any():
                vmin, vmax = float(v.min()), float(v.max())
                if vmin < vmax:
                    paso = max(1.0, (vmax - vmin) / 100.0)
                    r = st.slider(f"Rango de {colm}", min_value=vmin, max_value=vmax,
                                  value=(vmin, vmax), step=paso, key=keyp+"monto")
                    df = df[(pd.to_numeric(df[colm], errors="coerce") >= r[0]) &
                            (pd.to_numeric(df[colm], errors="coerce") <= r[1])]

        q = st.text_input("Búsqueda rápida (todas las columnas)", key=keyp+"q",
                          placeholder="Palabra clave, CT, entidad, título…")
        if q:
            mask = df.astype(str).apply(lambda s: s.str.contains(q, case=False, na=False)).any(axis=1)
            df = df[mask]

        cols = st.multiselect("Columnas a mostrar", options=list(df.columns),
                              default=list(df.columns), key=keyp+"cols")

    # Configuración visual de columnas
    col_cfg = {}
    if date_col:
        col_cfg[date_col] = st.column_config.DateColumn(date_col, help="Fecha")

    for c in money_cols:
        col_cfg[c] = st.column_config.NumberColumn(c, format="B/. %,.2f")

    if "Enlace" in df.columns:
        col_cfg["Enlace"] = st.column_config.LinkColumn("Enlace", display_text="Abrir", help="Abrir acto en PanamáCompra")

    # Tabla cómoda: ancho completo, altura mayor, encabezados fijos y sin índice
    st.dataframe(
        df[cols] if cols else df,
        hide_index=True,
        use_container_width=True,
        height=620,
        column_config=col_cfg,
    )

    st.caption(f"Mostrando {len(df)} filas")
    st.download_button(
        "⬇️ Descargar CSV",
        (df[cols] if cols else df).to_csv(index=False).encode("utf-8"),
        file_name=f"{sheet_name}.csv",
        mime="text/csv",
        key=keyp+"dl",
    )

# ---- UI en pestañas (más cómodo que selectbox) ----
tabs = st.tabs(TABS)
for tab, name in zip(tabs, TABS):
    with tab:
        st.subheader(name)
        df = load_df(name)
        if df.empty:
            st.info("Sin datos en esta pestaña.")
        else:
            render_df(df, name)
