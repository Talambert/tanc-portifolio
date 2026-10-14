"""Elementos de interface compartilhados pelas páginas do portal."""
import streamlit as st

from governanca.catalogo.service import Answer
from governanca.core.config import Settings


def init() -> None:
    """Estado da sessão. Na simulação tudo vive em memória; no modo real vai para o BigQuery."""
    state = st.session_state
    state.setdefault("settings", Settings())
    state.setdefault("audit", [])
    state.setdefault("applied_meta", {})   # chave "dataset.tabela.coluna" -> texto gravado
    state.setdefault("suggestions", [])
    state.setdefault("meta_gen", 0)        # muda a cada geração, para reiniciar o editor
    state.setdefault("profiles", {})
    state.setdefault("rules", [])
    state.setdefault("rules_gen", 0)
    state.setdefault("approved_rules", [])
    state.setdefault("published_rules", [])
    state.setdefault("chat", [])


def banner() -> None:
    if st.session_state.settings.simulation:
        st.info("🧪 **Modo simulação:** dados fictícios e nenhuma chamada ao GCP. "
                "A lógica de mascaramento, de avaliação de regras e de busca no catálogo é real.", icon=None)


def sidebar() -> None:
    settings = st.session_state.settings
    with st.sidebar:
        st.divider()
        st.caption("👤 demo@local")
        st.caption(f"{'🧪 Simulação' if settings.simulation else '☁️ Real'} · `{settings.project_id}`")
        st.caption("Protótipo · v0.1")


def render_answer(answer: Answer) -> None:
    """Mostra a resposta do catálogo com as fontes citadas."""
    if answer.abstained:
        st.warning(answer.text)
        return
    st.markdown(answer.text)
    with st.expander(f"📎 Fontes ({len(answer.citations)})"):
        for chunk, score in answer.citations:
            st.markdown(f"**{chunk.source}** · {chunk.reference} · relevância {score:.2f}")
            st.caption(chunk.text)
