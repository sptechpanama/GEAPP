import streamlit as st
import pandas as pd
from services.analysis_chat import answer_question
from core.config import DB_PATH
from pathlib import Path

st.set_page_config(page_title="Chat de datos", page_icon="💬", layout="wide")
st.title("💬 Chat con tus datos")

st.caption(f"Base actual: `{Path(DB_PATH) if DB_PATH else 'panamacompra.db'}`")

question = st.text_area(
    "Pregunta en lenguaje natural",
    placeholder="Ej: Muéstrame los actos mayores a 50,000 de este mes",
    height=120,
)

if st.button("Preguntar", type="primary", use_container_width=True):
    api_key = st.secrets.get("openai_api_key")
    if not api_key:
        st.error("Configura `openai_api_key` en secrets para usar el chat.")
    elif not question.strip():
        st.warning("Escribe una pregunta.")
    else:
        with st.spinner("Consultando..."):
            summary, df, raw = answer_question(question.strip(), api_key)
        st.write(summary)
        if df is not None and not df.empty:
            st.dataframe(df.head(50), use_container_width=True)
        with st.expander("Detalle de la respuesta del modelo"):
            st.code(raw, language="markdown")
