# ============================================================
# entities.py
# Selectores de Cliente y Proyecto con relación 1:N (Cliente → Proyectos)
# - client_selector: (ClienteID, ClienteNombre)
# - project_selector: (ProyectoID, ProyectoNombre, ClienteID_del_proyecto, ClienteNombre_del_proyecto)
#   Soporta 'selected_client_id' para filtrar proyectos por cliente.
# ============================================================

from __future__ import annotations
import streamlit as st
import pandas as pd
from typing import Tuple, Optional

from sheets import read_worksheet  # type: ignore

# Si tus nombres de hojas son distintos, cámbialos aquí:
WS_CLIENTES = "Clientes"
WS_PROYECTOS = "Proyectos"


# -------------------- Normalizadores --------------------
def _canon_client_cols(df: pd.DataFrame) -> pd.DataFrame:
    ren = {}
    if "ClienteID" not in df.columns:
        for c in ["ID", "Id", "client_id"]:
            if c in df.columns:
                ren[c] = "ClienteID"
                break
    if "ClienteNombre" not in df.columns:
        for c in ["Nombre", "Name", "DisplayName"]:
            if c in df.columns:
                ren[c] = "ClienteNombre"
                break
    return df.rename(columns=ren) if ren else df

def _canon_project_cols(df: pd.DataFrame) -> pd.DataFrame:
    ren = {}
    if "ProyectoID" not in df.columns:
        for c in ["ID", "Id", "project_id", "Proyecto"]:
            if c in df.columns:
                ren[c] = "ProyectoID"
                break
    if "ProyectoNombre" not in df.columns:
        for c in ["Nombre", "Name"]:
            if c in df.columns:
                ren[c] = "ProyectoNombre"
                break
    if "ClienteID" not in df.columns:
        for c in ["client_id", "IdCliente", "ID Cliente", "ID_Cliente", "Cliente"]:
            if c in df.columns:
                ren[c] = "ClienteID"
                break
    if "ClienteNombre" not in df.columns:
        for c in ["NombreCliente", "ClientName"]:
            if c in df.columns:
                ren[c] = "ClienteNombre"
                break
    return df.rename(columns=ren) if ren else df


# -------------------- Carga con cache --------------------
@st.cache_data(ttl=120, show_spinner=False)
def _load_clients(_client, sheet_id: str) -> pd.DataFrame:
    df = read_worksheet(_client, sheet_id, WS_CLIENTES)
    df = _canon_client_cols(df)
    for c in ["ClienteID", "ClienteNombre"]:
        if c not in df.columns:
            df[c] = ""
    df = df.drop_duplicates(subset=["ClienteID"]).sort_values("ClienteNombre", na_position="last")
    return df.reset_index(drop=True)

@st.cache_data(ttl=120, show_spinner=False)
def _load_projects(_client, sheet_id: str) -> pd.DataFrame:
    df = read_worksheet(_client, sheet_id, WS_PROYECTOS)
    df = _canon_project_cols(df)
    for c in ["ProyectoID", "ProyectoNombre", "ClienteID", "ClienteNombre"]:
        if c not in df.columns:
            df[c] = ""
    df = df.drop_duplicates(subset=["ProyectoID"]).sort_values("ProyectoNombre", na_position="last")
    return df.reset_index(drop=True)


# -------------------- UI Selectors --------------------
def client_selector(client, sheet_id: str, *, key: str) -> Tuple[str, str]:
    """Selectbox de cliente (id + nombre). Devuelve (ClienteID, ClienteNombre)."""
    df_cli = _load_clients(client, sheet_id)
    opciones = [""] + [f"{row.ClienteNombre} ▸ {row.ClienteID}" for _, row in df_cli.iterrows()]
    sel = st.selectbox("Cliente", opciones, index=0, key=f"{key}_cliente")
    if not sel:
        return "", ""
    nombre, _, cid = sel.partition(" ▸ ")
    return cid.strip(), nombre.strip()


def project_selector(
    client,
    sheet_id: str,
    *,
    key: str,
    allow_client_link: bool = True,
    selected_client_id: Optional[str] = None,
) -> Tuple[str, str, Optional[str], Optional[str]]:
    """
    Selectbox de proyecto. Si selected_client_id viene, filtra a solo proyectos de ese cliente.
    Devuelve (ProyectoID, ProyectoNombre, ClienteID_del_proyecto, ClienteNombre_del_proyecto)
    """
    df_proj = _load_projects(client, sheet_id)

    if selected_client_id:
        mask = df_proj["ClienteID"].astype(str).str.strip() == str(selected_client_id).strip()
        df_view = df_proj[mask].copy()
        if df_view.empty:
            opciones = ["", "— Sin proyectos para este cliente —"]
            st.selectbox("Proyecto", opciones, index=0, key=f"{key}_proyecto")
            return "", "", selected_client_id, None
    else:
        df_view = df_proj.copy()

    opciones = [""] + [f"{row.ProyectoNombre or row.ProyectoID} ▸ {row.ProyectoID}" for _, row in df_view.iterrows()]
    sel = st.selectbox("Proyecto", opciones, index=0, key=f"{key}_proyecto")

    if not sel:
        return "", "", None, None

    nombre, _, pid = sel.partition(" ▸ ")
    fila = df_proj[df_proj["ProyectoID"].astype(str) == pid.strip()].head(1)
    if fila.empty:
        return pid.strip(), nombre.strip(), None, None
    row = fila.iloc[0]
    return pid.strip(), nombre.strip(), (row["ClienteID"] or None), (row["ClienteNombre"] or None)
