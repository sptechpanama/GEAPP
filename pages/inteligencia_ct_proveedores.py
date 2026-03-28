from __future__ import annotations

import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import bcrypt

from ui.theme import apply_global_theme


st.set_page_config(
    page_title="Inteligencia CT y Proveedores",
    page_icon="🧠",
    layout="wide",
)
apply_global_theme()


# Guard de autenticacion (mismo patron que otras paginas)
USERS = {
    "rsanchez": ("Rodrigo Sánchez", "Sptech-71"),
    "isanchez": ("Irvin Sánchez", "Sptech-71"),
    "igsanchez": ("Iris Grisel Sánchez", "Sptech-71"),
}


def _hash(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()


credentials = {
    "usernames": {u: {"name": n, "password": _hash(p)} for u, (n, p) in USERS.items()}
}
COOKIE_NAME = "finapp_auth"
COOKIE_KEY = "finapp_key_123"
authenticator = stauth.Authenticate(credentials, COOKIE_NAME, COOKIE_KEY, 30)

try:
    authenticator.login(" ", location="sidebar", key="auth_intel_ct_silent")
    st.sidebar.empty()
except Exception:
    pass

if st.session_state.get("authentication_status") is not True:
    st.switch_page("Inicio.py")

authenticator.logout("Cerrar sesión", location="sidebar")


def _empty_table(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _placeholder_block(title: str, text: str, columns: list[str] | None = None) -> None:
    st.markdown(f"#### {title}")
    st.info(text)
    if columns:
        st.caption("Formato esperado de tabla (Fase 1, sin datos):")
        st.dataframe(_empty_table(columns), use_container_width=True, hide_index=True)


def _render_sidebar() -> None:
    st.sidebar.markdown("### 🎛️ Filtros globales")
    st.sidebar.selectbox("Ficha técnica", ["Todas"], index=0)
    st.sidebar.selectbox("Estado ficha", ["Todos"], index=0)
    st.sidebar.selectbox("Prioridad", ["Todas"], index=0)
    st.sidebar.selectbox("Proveedor", ["Todos"], index=0)
    st.sidebar.selectbox("País", ["Todos"], index=0)
    st.sidebar.selectbox("Clasificación de contacto", ["Todas"], index=0)
    st.sidebar.checkbox("Solo con contacto encontrado", value=False)
    st.sidebar.checkbox("Solo con seguimiento vencido", value=False)
    st.sidebar.checkbox("Solo ficha viable con proveedor en conversación", value=False)

    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚡ Acciones rápidas")
    st.sidebar.button("Recalcular scoring", disabled=True)
    st.sidebar.button("Actualizar tablero", disabled=True)
    st.sidebar.button("Exportar resumen", disabled=True)
    st.sidebar.button("Limpiar filtros", disabled=True)
    st.sidebar.caption("Fase 1: botones visuales (sin ejecución).")


def _render_kpis() -> None:
    st.markdown("### 📌 Centro de control")
    cols = st.columns(5)
    cols[0].metric("Fichas detectadas con actos en DB", "—")
    cols[1].metric("Fichas en seguimiento", "—")
    cols[2].metric("Fichas en estudio", "—")
    cols[3].metric("Seguimientos vencidos", "—")
    cols[4].metric("Correos por enviar", "—")

    cols2 = st.columns(5)
    cols2[0].metric("Ficha viable con proveedor en conversación", "—")
    cols2[1].metric("Ficha en estudio pendiente de contactar", "—")
    cols2[2].metric("Ficha en estudio sin encontrar proveedor", "—")
    cols2[3].metric("Ficha contactada no rentable", "—")
    cols2[4].metric("Justificaciones no rentables pendientes", "—")


def _render_tab_dashboard() -> None:
    st.markdown("### Dashboard Ejecutivo")
    _placeholder_block(
        "Resumen ejecutivo",
        "Aquí se mostrará un resumen textual automático del estado general del embudo comercial por ficha.",
    )
    _placeholder_block(
        "Alertas y tareas del día",
        "Aquí se mostrarán alertas (vencimientos, fichas sin avance, contactos pendientes) y tareas recomendadas.",
        ["tipo_alerta", "ficha", "proveedor", "prioridad", "fecha_limite", "accion_sugerida"],
    )
    _placeholder_block(
        "Top fichas por score",
        "Aquí se mostrará el ranking principal de fichas para atacar hoy.",
        ["ficha", "descripcion", "score_total", "clasificacion", "estado"],
    )


def _render_tab_deteccion_ct() -> None:
    st.markdown("### Detección automática de fichas")
    sub1, sub2, sub3 = st.tabs(["Scoring", "Resultados", "Detalle ficha"])

    with sub1:
        st.markdown("#### Ajuste de pesos del score")
        c1, c2, c3 = st.columns(3)
        c1.slider("Peso frecuencia", 0, 100, 20, disabled=True)
        c1.slider("Peso monto histórico", 0, 100, 20, disabled=True)
        c2.slider("Peso entidades", 0, 100, 15, disabled=True)
        c2.slider("Peso ganadores distintos", 0, 100, 10, disabled=True)
        c3.slider("Peso competencia", 0, 100, 10, disabled=True)
        c3.slider("Peso afinidad negocio", 0, 100, 15, disabled=True)
        st.slider("Peso barreras regulatorias/técnicas", 0, 100, 10, disabled=True)
        st.caption("Aquí se mostrará la suma total de pesos y validación automática.")
        b1, b2, b3, b4 = st.columns(4)
        b1.button("Recalcular", disabled=True)
        b2.button("Restaurar default", disabled=True)
        b3.button("Guardar configuración", disabled=True)
        b4.button("Cargar configuración", disabled=True)

    with sub2:
        _placeholder_block(
            "Tabla principal de detección de fichas",
            "Aquí se mostrarán las fichas detectadas con score y clasificación visual.",
            [
                "ficha",
                "descripcion",
                "frecuencia_actos",
                "monto_historico",
                "entidades_distintas",
                "ganadores_distintos",
                "competencia_promedio",
                "afinidad_negocio",
                "barreras",
                "score_total",
                "clasificacion",
            ],
        )
        st.caption("Acciones por fila (Fase 2): agregar a seguimiento, ignorar, ver detalle.")

    with sub3:
        _placeholder_block(
            "Descomposición del score por ficha",
            "Aquí se mostrará el detalle de cada factor del score para la ficha seleccionada.",
            ["ficha", "factor", "valor_normalizado", "peso", "contribucion_score"],
        )


def _render_tab_seguimiento_ct() -> None:
    st.markdown("### Fichas en seguimiento")
    _placeholder_block(
        "Pipeline de fichas",
        "Aquí se visualizará el pipeline manual de fichas seleccionadas para seguimiento.",
        ["ficha", "descripcion", "score_inicial", "prioridad_manual", "estado", "fecha_ingreso", "notas"],
    )
    st.caption("Estados sugeridos: pendiente, en estudio, listo para proveedores, pausado, descartado.")


def _render_tab_estudio_profundo() -> None:
    st.markdown("### Estudio profundo por ficha")
    st.selectbox("Selecciona ficha para estudio", ["(sin datos en Fase 1)"], index=0)
    _placeholder_block(
        "Acto por acto",
        "Aquí se mostrará el detalle completo de actos asociados a la ficha seleccionada.",
        [
            "fecha_publicacion",
            "fecha_adjudicacion",
            "dias_pub_a_adj",
            "tiempo_entrega",
            "entidad",
            "proveedor_participante",
            "proveedor_ganador",
            "marca",
            "modelo",
            "pais_origen",
            "precio_unitario_ofertado",
            "precio_unitario_ganador",
            "cantidad",
            "monto_total",
        ],
    )
    _placeholder_block(
        "KPIs consolidados y variación de precios",
        "Aquí se mostrarán promedio/min/max de adjudicación, marcas/paises frecuentes y variación % de precios.",
    )
    _placeholder_block(
        "Gráficos de análisis",
        "Aquí se mostrarán barras de precios por proveedor, frecuencia de victorias, marcas y países.",
    )


def _render_tab_proveedores_historicos_ia() -> None:
    st.markdown("### Proveedores históricos + IA")
    _placeholder_block(
        "Tabla histórica por proveedor",
        "Aquí se mostrarán solo proveedores con al menos una adjudicación en la ficha seleccionada.",
        [
            "proveedor",
            "participaciones",
            "victorias",
            "%_victorias",
            "precio_min",
            "precio_prom",
            "precio_max",
            "marcas",
            "modelos",
            "paises_origen",
            "entidades_donde_mas_gana",
        ],
    )
    _placeholder_block(
        "Bloque IA (interpretación ejecutiva)",
        "Aquí se mostrará el análisis IA: dominante, agresivo en precio, premium, concentración, posibilidad de entrada.",
    )


def _render_tab_contacto_correos() -> None:
    st.markdown("### Contacto y correos")
    _placeholder_block(
        "Generador de correo inicial",
        "Aquí se generará el correo inicial usando variables del proveedor, ficha y producto.",
        ["proveedor", "ficha", "asunto", "cuerpo_correo", "canal_sugerido"],
    )
    st.caption("Acciones visuales previstas: copiar correo, abrir mailto, abrir WhatsApp, marcar enviado.")


def _render_tab_seguimiento_contacto() -> None:
    st.markdown("### Seguimiento de contacto (CRM)")
    _placeholder_block(
        "Matriz de seguimiento",
        "Aquí se mostrará estado por proveedor/contacto/canal y días desde último contacto.",
        [
            "ficha",
            "proveedor",
            "canal_usado",
            "fecha_primer_contacto",
            "correo_enviado",
            "whatsapp_enviado",
            "contacto_exitoso_correo",
            "contacto_exitoso_whatsapp",
            "respuesta_recibida",
            "estado_actual",
            "dias_desde_ultimo_contacto",
        ],
    )
    _placeholder_block(
        "Automatización de follow-up",
        "Aquí se verán reglas automáticas de 2do y 3er contacto, con próximos pasos sugeridos.",
    )


def _render_tab_resultado_final() -> None:
    st.markdown("### Resultado final por ficha")
    row1_col1, row1_col2 = st.columns(2)
    with row1_col1:
        _placeholder_block(
            "Ficha viable con proveedor en conversación",
            "Aquí se listarán fichas viables con contacto activo y proveedor útil.",
            ["ficha", "proveedor", "marca", "modelo", "pais_origen", "email", "whatsapp", "estado_contacto", "precio", "observaciones"],
        )
    with row1_col2:
        _placeholder_block(
            "Ficha en estudio pendiente de contactar proveedor",
            "Aquí se listarán fichas que requieren primer contacto o siguiente acción inmediata.",
            ["ficha", "prioridad", "proveedor_objetivo", "canal_recomendado", "observaciones"],
        )

    row2_col1, row2_col2 = st.columns(2)
    with row2_col1:
        _placeholder_block(
            "Ficha en estudio sin encontrar proveedor",
            "Aquí se listarán fichas con intentos agotados sin proveedor confirmado.",
            ["ficha", "intentos_realizados", "canales_usados", "motivo_actual", "proximo_paso"],
        )
    with row2_col2:
        _placeholder_block(
            "Ficha contactada no rentable",
            "Aquí se mostrará justificación económica con proveedores contactados y razones.",
            [
                "ficha",
                "proveedores_contactados",
                "precio_obtenido",
                "rango_objetivo",
                "diferencia_%",
                "razones_no_rentable",
            ],
        )


def _render_architecture_notes() -> None:
    with st.expander("🧱 Arquitectura funcional propuesta (Fase 1 - diseño)", expanded=False):
        st.markdown(
            """
            - Esta página está montada como **blueprint visual** para validar flujo y UX.
            - En Fase 2 se conectará con tablas de datos (determinístico + IA).
            - Diseño preparado para módulos:
              - `core/ct_scoring.py`
              - `core/ct_analytics.py`
              - `services/ct_repository.py`
              - `services/ct_automation.py`
              - `services/ct_ai_insights.py`
            """
        )


st.markdown("# 🧠 Inteligencia de Prospección CT y Proveedores")
st.caption("Fase 1: arquitectura visual y textual (sin datos operativos).")

_render_sidebar()
_render_kpis()
_render_architecture_notes()

tabs = st.tabs(
    [
        "Dashboard",
        "Detección CT",
        "Fichas en seguimiento",
        "Estudio profundo por ficha",
        "Proveedores históricos + IA",
        "Contacto y correos",
        "Seguimiento de contacto",
        "Resultado final por ficha",
    ]
)

with tabs[0]:
    _render_tab_dashboard()
with tabs[1]:
    _render_tab_deteccion_ct()
with tabs[2]:
    _render_tab_seguimiento_ct()
with tabs[3]:
    _render_tab_estudio_profundo()
with tabs[4]:
    _render_tab_proveedores_historicos_ia()
with tabs[5]:
    _render_tab_contacto_correos()
with tabs[6]:
    _render_tab_seguimiento_contacto()
with tabs[7]:
    _render_tab_resultado_final()
