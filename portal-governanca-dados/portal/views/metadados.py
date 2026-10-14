import uuid

import pandas as pd
import streamlit as st

from governanca.core import audit, masking
from governanca.metadados import service
from governanca.mock import data as mock
from portal import ui

ui.init()
state = st.session_state
settings = state.settings

st.title("🏷️ Metadados")
st.caption("O modelo propõe descrições e classificações a partir do contexto do dado. "
           "Você revisa, edita e aprova. Nada é gravado sem a sua confirmação.")
ui.banner()

# ---------- 1. Escolha das tabelas ----------
c1, c2 = st.columns([1, 2])
dataset = c1.selectbox("Dataset", service.list_datasets())
available = service.list_tables(dataset)
tables = c2.multiselect("Tabelas", available, default=available)

if tables:
    with st.expander("🔬 O que o modelo recebe (esquema e amostra mascarada)"):
        table = st.selectbox("Tabela", tables, key="contexto_tabela")
        schema = pd.DataFrame(service.schema_of(dataset, table), columns=["Coluna", "Tipo", "Descrição existente"])
        st.markdown("**Esquema e metadados existentes**")
        st.dataframe(schema, hide_index=True)
        st.markdown(f"**Amostra** (até {settings.max_sample_rows} linhas, dados sensíveis mascarados)")
        st.dataframe(masking.mask_dataframe(mock.get_sample(dataset, table, dirty=True)).head(settings.max_sample_rows),
                     hide_index=True)

if st.button("✨ Gerar sugestões", type="primary", disabled=not tables):
    with st.status("Montando contexto e gerando sugestões...", expanded=False) as status:
        state.suggestions = service.generate(dataset, tables)
        state.meta_gen += 1
        status.update(label=f"{len(state.suggestions)} sugestões geradas", state="complete")

# ---------- 2. Revisão ----------
items: list[service.Suggestion] = state.suggestions
if items:
    st.subheader("Revisão")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Sugestões", len(items))
    m2.metric("Pré-selecionadas", sum(s.apply_by_default for s in items))
    m3.metric("Baixa confiança", sum(s.confidence == "baixa" for s in items))
    m4.metric("Texto humano a preservar", sum(s.has_human_text for s in items))
    st.caption("Descrições escritas por pessoas **não** vêm marcadas: marque *Aplicar* para confirmar a troca. "
               "Itens de baixa confiança também ficam desmarcados. A coluna *Proposto* pode ser editada.")

    frame = pd.DataFrame([{
        "Aplicar": s.apply_by_default,
        "Tabela": s.table,
        "Coluna": s.column or "(tabela)",
        "Existente": s.existing,
        "Proposto": s.proposed,
        "Confiança": s.confidence,
        "Sensível?": "⚠️ sim" if s.sensitive else "",
        "Classificação": s.classification,
        "Evidência": s.evidence,
    } for s in items])

    edited = st.data_editor(
        frame, hide_index=True, key=f"meta_editor_{state.meta_gen}",
        disabled=[c for c in frame.columns if c not in ("Aplicar", "Proposto")],
        column_config={"Aplicar": st.column_config.CheckboxColumn(width="small"),
                       "Proposto": st.column_config.TextColumn(width="large"),
                       "Evidência": st.column_config.TextColumn(width="large")},
    )

    a, b, _ = st.columns([1, 1, 3])
    if a.button("✅ Aprovar e gravar (simulado)", type="primary"):
        batch = uuid.uuid4().hex[:8]
        written = 0
        for index, row in edited.iterrows():
            text = str(row["Proposto"]).strip()
            if not row["Aplicar"] or not text:
                continue
            s = items[index]
            before = state.applied_meta.get(s.key, s.existing)
            state.applied_meta[s.key] = text
            audit.record(state.audit, "Gravar descrição", s.key, before=before, after=text, batch=batch)
            written += 1
        if written:
            st.success(f"{written} descrições gravadas (simulado) e registradas na auditoria. "
                       "Elas já aparecem nas respostas do Catálogo.")
        else:
            st.warning("Nenhum item marcado com texto para gravar.")

    if b.button("↩️ Desfazer última aprovação"):
        batch = audit.last_open_batch(state.audit, "Gravar descrição")
        if batch is None:
            st.info("Não há aprovação para desfazer.")
        else:
            for entry in reversed(state.audit):
                if entry.batch == batch and entry.action == "Gravar descrição" and not entry.undone:
                    if entry.before:
                        state.applied_meta[entry.target] = entry.before
                    else:
                        state.applied_meta.pop(entry.target, None)
                    entry.undone = True
            audit.record(state.audit, "Desfazer aprovação", f"lote {batch}")
            st.success("Última aprovação desfeita.")
else:
    st.caption("Escolha as tabelas e clique em **Gerar sugestões**.")

# ---------- 3. Auditoria ----------
history = [e for e in state.audit if e.action == "Gravar descrição"]
if history:
    with st.expander(f"🧾 Auditoria de gravações ({len(history)})"):
        st.dataframe(pd.DataFrame([{
            "Quando": e.at, "Quem": e.user, "Objeto": e.target, "Antes": e.before, "Depois": e.after,
            "Desfeito": "sim" if e.undone else "",
        } for e in reversed(history)]), hide_index=True)
