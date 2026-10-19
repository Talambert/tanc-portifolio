import pandas as pd
import streamlit as st

from governanca.catalogo import service as catalogo
from governanca.metadados import service as metadados
from governanca.mock import data as mock
from portal import ui

ui.init()
state = st.session_state

ui.page_header("Portal de Governança de Dados",
               "Documente, proteja e consulte seus dados com apoio de IA, sempre com revisão humana.")

# ---------- Cobertura de documentação ----------
baseline = metadados.documentation_coverage()
current = metadados.documentation_coverage(state.applied_meta)
total = sum(r["objetos"] for r in current)
doc_now = sum(r["documentados"] for r in current)
doc_before = sum(r["documentados"] for r in baseline)
coverage_now = doc_now / total
delta = (doc_now - doc_before) / total

ui.kpis([
    {"label": "Cobertura", "value": f"{coverage_now:.0%}", "delta": f"{delta:+.0%}" if delta else None,
     "help": "Cobertura de documentação: tabelas e colunas com descrição, entre todas as catalogadas."},
    {"label": "A documentar", "value": total - doc_now, "help": "Tabelas e colunas ainda sem descrição."},
    {"label": "Gravadas pelo portal", "value": len(state.applied_meta), "help": "Descrições gravadas por este portal."},
    {"label": "Scans ativos", "value": len(state.published_rules), "help": "Scans de qualidade publicados."},
])

left, right = st.columns([3, 2], gap="large")

with left:
    st.header("Documentação por tabela", anchor=False)
    frame = pd.DataFrame(current)
    frame["Tabela"] = frame["dataset"] + "." + frame["tabela"]
    frame["Documentados"] = frame["documentados"].astype(str) + " de " + frame["objetos"].astype(str)
    frame["Cobertura"] = frame["documentados"] / frame["objetos"] * 100
    st.dataframe(
        frame.sort_values("Cobertura")[["Tabela", "Documentados", "Cobertura"]], hide_index=True,
        column_config={
            "Tabela": st.column_config.TextColumn(width="medium"),
            "Cobertura": st.column_config.ProgressColumn("Cobertura", min_value=0, max_value=100, format="%.0f%%"),
        },
    )
    st.caption("Ordenado da menor para a maior cobertura. Comece pelas primeiras linhas.")

with right:
    st.header("Pergunte ao catálogo", anchor=False)
    with st.form("busca_rapida", border=False):
        question = st.text_input("Pergunta", placeholder="Ex.: o que significa o valor de compra?", label_visibility="collapsed")
        submitted = st.form_submit_button("Buscar", icon=":material/search:")
    if submitted and question.strip():
        ok, answer = ui.run("Responder pergunta", catalogo.answer, question, catalogo.build_corpus(state.applied_meta))
        if ok:
            ui.render_answer(answer)

st.header("Por onde começar", anchor=False)
a, b, c = st.columns(3)
for column, page, icon, title, text in (
    (a, "views/metadados.py", ":material/sell:", "Metadados", "Gere descrições e classificações para tabelas sem documentação."),
    (b, "views/qualidade.py", ":material/fact_check:", "Data Quality", "Faça o profiling, proponha regras e publique os scans."),
    (c, "views/catalogo.py", ":material/menu_book:", "Catálogo", "Perguntas em português, com a fonte citada."),
):
    with column.container(border=True):
        st.markdown(f"**{title}**")
        st.caption(text)
        st.page_link(page, label=f"Abrir {title}", icon=icon)

with st.expander("Como ler este protótipo", icon=":material/info:"):
    st.markdown(
        "**Real:** mascaramento de dados sensíveis, avaliação das regras de qualidade, busca no catálogo com citação "
        "e recusa, trilha de auditoria com desfazer.  \n"
        "**Simulado:** as sugestões de metadados, as propostas de regras e os dados (nenhuma chamada ao GCP).  \n"
        "**Ainda não existe:** profiling e scans no Dataplex, Gemini, embeddings e o deploy no Cloud Run."
    )

st.header("Atividade recente", anchor=False)
if state.audit:
    rows = [{"Quando": e.at, "Quem": e.user, "Ação": e.action, "Objeto": e.target, "Desfeito": "sim" if e.undone else ""}
            for e in reversed(state.audit[-10:])]
    st.dataframe(pd.DataFrame(rows), hide_index=True)
else:
    st.caption("Nenhuma atividade ainda. As aprovações feitas nos módulos aparecem aqui.")
