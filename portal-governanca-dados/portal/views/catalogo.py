import streamlit as st

from governanca.catalogo import service
from portal import ui

ui.init()
state = st.session_state

EXAMPLES = [
    "O que significa o valor de compra?",
    "De onde vem a tabela de preços?",
    "Quando compensa abastecer com etanol?",
    "Qual a cotação do dólar hoje?",
]


def _pick_example() -> None:
    state.pending_question = state.example_choice
    state.example_choice = None  # solta a seleção para o mesmo exemplo poder ser escolhido de novo


ui.page_header(
    "Catálogo",
    "Pergunte em português sobre os dados. A resposta usa só o que está nas fontes, cita de onde veio "
    "e diz \"não encontrei\" quando a fonte não cobre a pergunta.",
)

corpus = service.build_corpus(state.applied_meta)
from_portal = sum(c.source.startswith("Metadados (gravados") for c in corpus)
with st.container(horizontal=True, vertical_alignment="center"):
    st.badge(f"{len(corpus)} trechos indexados", icon=":material/database:", color="gray")
    st.badge(f"{from_portal} vindos do módulo de Metadados", icon=":material/sell:", color="green" if from_portal else "gray")

st.pills("Exemplos", EXAMPLES, key="example_choice", on_change=_pick_example, label_visibility="collapsed")

question = st.chat_input("Pergunte sobre os dados...") or state.pop("pending_question", None)
if question:
    state.chat.append({"role": "user", "content": question})
    with st.spinner("Buscando nas fontes..."):
        ok, result = ui.run("Responder pergunta", service.answer, question, corpus, silent=True)
    state.chat.append({"role": "assistant", "answer": result} if ok else {"role": "assistant", "error": result})

if not state.chat:
    st.info("Faça uma pergunta ou escolha um dos exemplos acima. Tente também uma pergunta fora do escopo, "
            "para ver o catálogo recusar em vez de inventar.", icon=":material/chat:")

for message in state.chat:
    avatar = ":material/person:" if message["role"] == "user" else ":material/menu_book:"
    with st.chat_message(message["role"], avatar=avatar):
        if message["role"] == "user":
            st.markdown(message["content"])
        elif "error" in message:
            ui.show_error(message["error"])
        else:
            ui.render_answer(message["answer"])

if state.chat and st.button("Limpar conversa", icon=":material/delete_sweep:", width="content"):
    state.chat = []
    st.rerun()
