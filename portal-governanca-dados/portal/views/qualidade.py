import copy
import uuid

import pandas as pd
import streamlit as st

from governanca.core import audit
from governanca.mock import data as mock
from governanca.qualidade import service
from portal import ui

ui.init()
state = st.session_state

st.title("✅ Data Quality")
st.caption("Profiling do dado, regras propostas pela IA e testadas antes de publicar. "
           "As regras viram código (YAML) e só viram *scans* depois da sua aprovação.")
ui.banner()

c1, c2 = st.columns([1, 2])
dataset = c1.selectbox("Dataset", mock.list_datasets())
available = mock.list_tables(dataset)
tables = c2.multiselect("Tabelas", available, default=available)

tab_profile, tab_rules, tab_publish = st.tabs(["1. Profiling", "2. Regras", "3. Publicação e resultados"])

# ---------- 1. Profiling ----------
with tab_profile:
    st.caption("O profiling mostra como o dado realmente se comporta: nulos, valores distintos e faixas.")
    if st.button("📊 Executar profiling", type="primary", disabled=not tables):
        progress = st.progress(0.0, text="Executando profiling...")
        for i, table in enumerate(tables, 1):
            state.profiles[(dataset, table)] = service.profile(dataset, table)
            progress.progress(i / len(tables), text=f"Profiling de {table} ({i}/{len(tables)})")
        progress.empty()
    shown = [t for t in tables if (dataset, t) in state.profiles]
    for table in shown:
        with st.expander(f"📋 {table}", expanded=len(shown) == 1):
            st.dataframe(state.profiles[(dataset, table)], hide_index=True, column_config={
                "% nulos": st.column_config.ProgressColumn("% nulos", min_value=0, max_value=100, format="%.1f%%")})
    if not shown:
        st.caption("Nenhum profiling executado ainda.")

# ---------- 2. Regras ----------
with tab_rules:
    st.caption("O Gemini propõe regras com base no profiling e na amostra, e o portal as compara com as "
               "recomendações do Dataplex. Cada regra é testada em **dado limpo** antes de ser publicada.")
    if st.button("🤖 Propor regras", type="primary", disabled=not tables):
        with st.status("Propondo e testando regras...", expanded=False) as status:
            state.rules = service.propose(dataset, tables)
            state.rules_gen += 1
            state.approved_rules = []
            status.update(label=f"{len(state.rules)} regras propostas e testadas", state="complete")

    rules: list[service.Rule] = state.rules
    if rules:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Regras propostas", len(rules))
        m2.metric("Origem: Gemini", sum(r.origin == "gemini" for r in rules))
        m3.metric("Origem: Dataplex", sum(r.origin == "dataplex_recomendada" for r in rules))
        m4.metric("Falsos alarmes no teste", sum(r.false_alarm for r in rules),
                  help="Regras que falham mesmo em dado limpo; ficam desmarcadas.")

        frame = pd.DataFrame([{
            "Aplicar": not r.false_alarm and r.confidence != "baixa",
            "Tabela": r.table,
            "Coluna": r.column,
            "Regra": service.KINDS[r.kind],
            "Parâmetros": r.params_text,
            "Limiar": r.threshold,
            "Origem": "Gemini" if r.origin == "gemini" else "Dataplex",
            "Confiança": r.confidence,
            "Teste em dado limpo": round(r.dry_run_rate * 100, 1),
            "Falso alarme?": "⚠️ sim" if r.false_alarm else "não",
            "Justificativa": r.justification,
        } for r in rules])

        edited = st.data_editor(
            frame, hide_index=True, key=f"rules_editor_{state.rules_gen}",
            disabled=[c for c in frame.columns if c not in ("Aplicar", "Limiar")],
            column_config={
                "Aplicar": st.column_config.CheckboxColumn(width="small"),
                "Limiar": st.column_config.NumberColumn(min_value=0.5, max_value=1.0, step=0.01, format="%.2f"),
                "Teste em dado limpo": st.column_config.NumberColumn(format="%.1f%%"),
                "Justificativa": st.column_config.TextColumn(width="large"),
            },
        )
        if st.button("✅ Aprovar regras selecionadas"):
            approved = []
            for index, row in edited.iterrows():
                if row["Aplicar"]:
                    rule = copy.copy(rules[index])
                    rule.threshold = float(row["Limiar"])
                    approved.append(rule)
            state.approved_rules = approved
            audit.record(state.audit, "Aprovar regras", f"{len(approved)} regras em {dataset}", batch=uuid.uuid4().hex[:8])
            st.success(f"{len(approved)} regras aprovadas. Siga para a aba **3. Publicação e resultados**.")
    else:
        st.caption("Clique em **Propor regras**.")

# ---------- 3. Publicação e resultados ----------
with tab_publish:
    approved = state.approved_rules
    if not approved:
        st.caption("Aprove regras na etapa 2 para publicá-las.")
    else:
        st.markdown(f"**{len(approved)} regras aprovadas.** Regras como código, prontas para o git:")
        yaml_text = service.to_yaml(approved)
        st.code(yaml_text, language="yaml")
        left, right = st.columns([1, 1])
        left.download_button("⬇️ Baixar YAML", yaml_text, file_name=f"regras_{dataset}.yaml", mime="text/yaml")
        if right.button("🚀 Publicar scans no Dataplex (simulado)", type="primary"):
            state.published_rules = approved
            audit.record(state.audit, "Publicar scans", f"{len(approved)} scans em {dataset}", batch=uuid.uuid4().hex[:8])
            st.success("Scans publicados (simulado) com execução agendada.")

    published: list[service.Rule] = state.published_rules
    if published:
        st.divider()
        st.subheader("Resultados dos scans")
        st.caption("O dado atual tem defeitos injetados de propósito (nulos, formatos, faixas, duplicidades). "
                   "As violações abaixo são o que as regras conseguem pegar.")
        results = pd.DataFrame([{
            "Regra": f"{r.table}.{r.column} ({service.KINDS[r.kind]})",
            "Taxa de aprovação (%)": round(service.scan(r) * 100, 1),
            "Limiar (%)": round(r.threshold * 100, 1),
        } for r in published])
        results["Status"] = ["✅ OK" if a >= b else "❌ Violação" for a, b in zip(results.iloc[:, 1], results.iloc[:, 2])]

        k1, k2, k3 = st.columns(3)
        k1.metric("Scans publicados", len(results))
        k2.metric("Violações detectadas", int((results["Status"] == "❌ Violação").sum()))
        k3.metric("Regras sem violação", int((results["Status"] == "✅ OK").sum()))
        st.bar_chart(results.set_index("Regra")["Taxa de aprovação (%)"])
        st.dataframe(results, hide_index=True)
