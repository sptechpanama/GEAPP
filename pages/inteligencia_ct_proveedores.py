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
    st.sidebar.selectbox("CT", ["Todos"], index=0)
    st.sidebar.selectbox("Estado CT", ["Todos"], index=0)
    st.sidebar.selectbox("Prioridad", ["Todas"], index=0)
    st.sidebar.selectbox("Proveedor", ["Todos"], index=0)
    st.sidebar.selectbox("País", ["Todos"], index=0)
    st.sidebar.selectbox("Clasificación comercial", ["Todas"], index=0)
    st.sidebar.checkbox("Solo con contacto encontrado", value=False)
    st.sidebar.checkbox("Solo con seguimiento vencido", value=False)
    st.sidebar.checkbox("Solo con proveedor aprobado", value=False)

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
    cols[0].metric("CT detectados", "—")
    cols[1].metric("CT en seguimiento", "—")
    cols[2].metric("CT en estudio", "—")
    cols[3].metric("Proveedores externos", "—")
    cols[4].metric("Seguimientos vencidos", "—")

    cols2 = st.columns(5)
    cols2[0].metric("CT activos con proveedor útil", "—")
    cols2[1].metric("CT sin proveedor", "—")
    cols2[2].metric("CT no rentable", "—")
    cols2[3].metric("Contactos pendientes", "—")
    cols2[4].metric("Correos por enviar", "—")


def _render_tab_dashboard() -> None:
    st.markdown("### Dashboard Ejecutivo")
    _placeholder_block(
        "Resumen ejecutivo",
        "Aquí se mostrará un resumen textual automático del estado general del embudo comercial CT.",
    )
    _placeholder_block(
        "Alertas y tareas del día",
        "Aquí se mostrarán alertas (vencimientos, CT sin avance, contactos pendientes) y tareas recomendadas.",
        ["tipo_alerta", "ct", "proveedor", "prioridad", "fecha_limite", "accion_sugerida"],
    )
    _placeholder_block(
        "Top CT por score",
        "Aquí se mostrará el ranking principal de CT para atacar hoy.",
        ["ct", "descripcion", "score_total", "clasificacion", "estado"],
    )


def _render_tab_deteccion_ct() -> None:
    st.markdown("### Detección automática de CT")
    sub1, sub2, sub3 = st.tabs(["Scoring", "Resultados", "Detalle CT"])

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
            "Tabla principal de detección CT",
            "Aquí se mostrarán los CT detectados con score y clasificación visual.",
            [
                "ct",
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
            "Descomposición del score por CT",
            "Aquí se mostrará el detalle de cada factor del score para el CT seleccionado.",
            ["ct", "factor", "valor_normalizado", "peso", "contribucion_score"],
        )


def _render_tab_seguimiento_ct() -> None:
    st.markdown("### CT en seguimiento")
    _placeholder_block(
        "Pipeline de CT",
        "Aquí se visualizará el pipeline manual de CT seleccionados para seguimiento.",
        ["ct", "descripcion", "score_inicial", "prioridad_manual", "estado", "fecha_ingreso", "notas"],
    )
    st.caption("Estados sugeridos: pendiente, en estudio, listo para proveedores, pausado, descartado.")


def _render_tab_estudio_profundo() -> None:
    st.markdown("### Estudio profundo por CT")
    st.selectbox("Selecciona CT para estudio", ["(sin datos en Fase 1)"], index=0)
    _placeholder_block(
        "Acto por acto",
        "Aquí se mostrará el detalle completo de actos asociados al CT seleccionado.",
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
        "Aquí se mostrarán solo proveedores con al menos una adjudicación en el CT seleccionado.",
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


def _render_tab_proveedores_externos() -> None:
    st.markdown("### Proveedores externos")
    _placeholder_block(
        "Formulario y registro",
        "Aquí se capturarán proveedores externos por CT y su perfil comercial.",
        [
            "proveedor",
            "fuente",
            "pais",
            "enlace",
            "producto",
            "marca",
            "modelo",
            "pais_origen",
            "valor_agregado",
            "clasificacion_comercial",
            "moq",
            "precio_visible",
            "email",
            "whatsapp",
            "sitio_web",
            "observaciones",
        ],
    )


def _render_tab_contacto_correos() -> None:
    st.markdown("### Contacto y correos")
    _placeholder_block(
        "Generador de correo inicial",
        "Aquí se generará el correo inicial usando variables del proveedor, CT y producto.",
        ["proveedor", "ct", "asunto", "cuerpo_correo", "canal_sugerido"],
    )
    st.caption("Acciones visuales previstas: copiar correo, abrir mailto, abrir WhatsApp, marcar enviado.")


def _render_tab_seguimiento_contacto() -> None:
    st.markdown("### Seguimiento de contacto (CRM)")
    _placeholder_block(
        "Matriz de seguimiento",
        "Aquí se mostrará estado por proveedor/contacto/canal y días desde último contacto.",
        [
            "ct",
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
    st.markdown("### Resultado final por CT")
    col1, col2, col3 = st.columns(3)
    with col1:
        _placeholder_block(
            "CT activo con proveedor útil",
            "Aquí se listarán CT viables y listos para operar.",
            ["ct", "proveedor", "marca", "modelo", "pais_origen", "email", "whatsapp", "estado_contacto", "precio", "observaciones"],
        )
    with col2:
        _placeholder_block(
            "CT sin proveedor conseguido",
            "Aquí se listarán CT con intentos agotados sin proveedor confirmado.",
            ["ct", "intentos", "canales_usados", "observaciones"],
        )
    with col3:
        _placeholder_block(
            "CT con proveedor pero no rentable",
            "Aquí se listarán CT descartados por precio/margen no viable.",
            ["ct", "proveedor", "precio_obtenido", "rango_objetivo", "diferencia_%", "motivo_descarte"],
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
        "CT en seguimiento",
        "Estudio profundo por CT",
        "Proveedores históricos + IA",
        "Proveedores externos",
        "Contacto y correos",
        "Seguimiento de contacto",
        "Resultado final por CT",
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
    _render_tab_proveedores_externos()
with tabs[6]:
    _render_tab_contacto_correos()
with tabs[7]:
    _render_tab_seguimiento_contacto()
with tabs[8]:
    _render_tab_resultado_final()

