"""Portal de Governança de Dados: ponto de entrada e navegação.

Rodar a partir da pasta do projeto:  streamlit run portal/app.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for path in (ROOT, ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import streamlit as st  # noqa: E402

from portal import ui  # noqa: E402

st.set_page_config(page_title="Portal de Governança de Dados", page_icon="🛡️", layout="wide")
ui.init()

navigation = st.navigation(
    {
        "": [st.Page("views/inicio.py", title="Início", icon="🏠", default=True)],
        "Governança": [
            st.Page("views/metadados.py", title="Metadados", icon="🏷️"),
            st.Page("views/qualidade.py", title="Data Quality", icon="✅"),
            st.Page("views/catalogo.py", title="Catálogo", icon="📚"),
        ],
        "Sistema": [st.Page("views/configuracoes.py", title="Configurações", icon="⚙️")],
    }
)
ui.sidebar()
navigation.run()
