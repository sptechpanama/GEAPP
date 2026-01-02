from __future__ import annotations

import streamlit as st


def apply_global_theme() -> None:
    """Inyecta un tema oscuro/gradiente unificado para todas las páginas."""
    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700&display=swap');
:root {
  --pc-bg: #0b1224;
  --pc-surface: #0f172a;
  --pc-border: rgba(255,255,255,0.08);
  --pc-accent: #22c55e;
  --pc-accent-2: #0ea5e9;
  --pc-text: #e7edf7;
  --pc-muted: #9fb2c7;
}
.stApp {
  background: radial-gradient(140% 120% at 18% 10%, #1c3d7133 0%, transparent 40%),
              radial-gradient(120% 120% at 80% 0%, #0ea5e926 0%, transparent 45%),
              linear-gradient(125deg, #0b1224 0%, #0c1a30 45%, #10223f 100%);
  color: var(--pc-text);
  font-family: 'Manrope', system-ui, -apple-system, sans-serif;
}
.block-container { padding-top: 1.1rem; max-width: 1200px; }
h1, h2, h3, h4 { color: var(--pc-text); letter-spacing: -0.015em; }
label { color: #cdd6e5 !important; font-weight: 600; }
[data-testid="stMarkdown"] a { color: var(--pc-accent-2); text-decoration: none; }
[data-testid="stMarkdown"] a:hover { text-decoration: underline; }

div.stButton>button,
[data-testid="stForm"] button,
[data-testid="stFormSubmitButton"] button {
  background: linear-gradient(135deg, var(--pc-accent-2), var(--pc-accent));
  color: #f8fbff; border: 1px solid rgba(255,255,255,0.15);
  border-radius: 10px; padding: 0.45rem 0.85rem; font-weight: 700;
  box-shadow: 0 8px 24px rgba(14,165,233,0.18);
}
div.stButton>button:hover,
[data-testid="stForm"] button:hover,
[data-testid="stFormSubmitButton"] button:hover {
  transform: translateY(-1px);
  box-shadow: 0 12px 30px rgba(34,197,94,0.28);
}

div[data-testid="stExpander"] { background: rgba(255,255,255,0.04); border: 1px solid var(--pc-border); border-radius: 14px; }
div[data-testid="stExpander"] summary { color: var(--pc-text); font-weight: 700; }
div[data-testid="stExpander"] > details { background: var(--pc-surface); border-radius: 12px; overflow: hidden; border: 1px solid var(--pc-border); }
div[data-testid="stExpander"] > details > summary { background: linear-gradient(120deg, rgba(14,165,233,0.12), rgba(34,197,94,0.10)); color: var(--pc-text); padding: 10px 14px; border-bottom: 1px solid var(--pc-border); }
div[data-testid="stExpander"] > details[open] > summary { background: linear-gradient(120deg, rgba(14,165,233,0.16), rgba(34,197,94,0.14)); }
div[data-testid="stExpander"] > details > div[role="group"] { background: #0c1528; padding: 12px 14px 16px; }

.stTextInput>div>div>input, .stTextArea textarea, [data-baseweb="select"]>div {
  background: #0f172a; color: var(--pc-text); border: 1px solid var(--pc-border); border-radius: 10px;
  box-shadow: inset 0 0 0 1px rgba(14,165,233,0.08);
}
.stDataFrame, [data-testid="stDataEditor"] {
  background: rgba(15,23,42,0.45); border: 1px solid var(--pc-border); border-radius: 12px;
}
.stDataFrame table, .stDataFrame tbody tr, .stDataFrame tbody td { background: transparent !important; color: #e4e9f3; }
.stDataFrame tbody tr:nth-child(odd) { background: rgba(255,255,255,0.02); }
.stDataFrame tbody tr:hover { background: rgba(14,165,233,0.08); }
</style>
""",
        unsafe_allow_html=True,
    )
