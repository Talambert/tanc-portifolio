import pandas as pd
import streamlit as st

from governanca.catalogo import service as catalogo
from governanca.mock import data as mock
from portal import ui

ui.init()
state = st.session_state

st.title("🛡️ Portal de Governança de Dados")
st.caption("Documente, proteja e consulte seus dados com apoio de IA, sempre com revisão humana.")
ui.banner()

tables = sum(len(t) for t in mock.DATASETS.values())
c1, c2, c3, c4 = st.columns(4)
c1.metric("Datasets", len(mock.DATASETS))
c2.metric("Tabelas catalogadas", tables)
c3.metric("Descrições gravadas", len(state.applied_meta))
c4.metric("Scans de qualidade", len(state.published_rules))

st.subheader("🔎 Pergunte ao catálogo")
with st.form("busca_rapida", clear_on_submit=False):
    question = st.text_input("Pergunta", placeholder="Ex.: o que significa o valor de compra?", label_visibility="collapsed")
    submitted = st.form_submit_button("Buscar")
if submitted and question.strip():
    ui.render_answer(catalogo.answer(question, catalogo.build_corpus(state.applied_meta)))

st.subheader("🚀 Por onde começar")
a, b, c = st.columns(3)
with a:
    st.markdown("**🏷️ Metadados**\n\nGere descrições e classificações para tabelas sem documentação.")
    st.page_link("views/metadados.py", label="Abrir Metadados", icon="🏷️")
with b:
    st.markdown("**✅ Data Quality**\n\nFaça o profiling, proponha regras e publique os scans.")
    st.page_link("views/qualidade.py", label="Abrir Data Quality", icon="✅")
with c:
    st.markdown("**📚 Catálogo**\n\nPerguntas em português, com a fonte citada.")
    st.page_link("views/catalogo.py", label="Abrir Catálogo", icon="📚")

st.subheader("🧾 Atividade recente")
if state.audit:
    rows = [{"Quando": e.at, "Quem": e.user, "Ação": e.action, "Objeto": e.target, "Desfeito": "sim" if e.undone else ""}
            for e in reversed(state.audit[-10:])]
    st.dataframe(pd.DataFrame(rows), hide_index=True)
else:
    st.caption("Nenhuma atividade ainda. As aprovações feitas nos módulos aparecem aqui.")
