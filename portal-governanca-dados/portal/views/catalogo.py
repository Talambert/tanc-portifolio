import streamlit as st

from governanca.catalogo import service
from portal import ui

ui.init()
state = st.session_state

st.title("📚 Catálogo")
st.caption("Pergunte em português sobre os dados. A resposta usa **só o que está nas fontes**, "
           "cita de onde veio e diz \"não encontrei\" quando a fonte não cobre a pergunta.")
ui.banner()

corpus = service.build_corpus(state.applied_meta)
from_portal = sum(c.source.startswith("Metadados (gravados") for c in corpus)
st.caption(f"📦 {len(corpus)} trechos indexados · {from_portal} vindos do módulo de Metadados")

examples = [
    "O que significa o valor de compra?",
    "De onde vem a tabela de preços?",
    "Quando compensa abastecer com etanol?",
    "Qual a cotação do dólar hoje?",
]
st.write("Exemplos:")
columns = st.columns(len(examples))
for column, example in zip(columns, examples):
    if column.button(example):
        state.pending_question = example

question = st.chat_input("Pergunte sobre os dados...") or state.pop("pending_question", None)
if question:
    answer = service.answer(question, corpus)
    state.chat.append({"role": "user", "content": question})
    state.chat.append({"role": "assistant", "answer": answer})

for message in state.chat:
    with st.chat_message(message["role"]):
        if message["role"] == "user":
            st.markdown(message["content"])
        else:
            ui.render_answer(message["answer"])

if state.chat and st.button("🧹 Limpar conversa"):
    state.chat = []
    st.rerun()
