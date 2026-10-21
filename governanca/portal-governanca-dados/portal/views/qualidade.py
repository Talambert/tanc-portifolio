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
state.setdefault("q_decisions", {})   # chave da regra -> {"apply": bool, "limit": float}: fonte da verdade da revisão

CONFIDENCE_COLOR = {"alta": "green", "média": "orange", "baixa": "red"}
COLUMNS = [0.7, 2.7, 1.5, 1.3, 1.8, 4.2]  # Aplicar · Regra · Origem · Limiar · Teste em dado limpo · Justificativa
PAGE = 6
ORIGINS = {"Gemini": "gemini", "Dataplex": "dataplex_recomendada"}

# Como em Metadados, as decisões ficam em `q_decisions` (e não nos widgets), para filtros e "mostrar mais" não as perderem.


def _wkey(kind: str, r: service.Rule) -> str:
    return f"q_{kind}_{state.rules_gen}_{r.key}"


def _on_apply(r: service.Rule) -> None:
    state.q_decisions[r.key]["apply"] = bool(state[_wkey("apply", r)])


def _on_limit(r: service.Rule) -> None:
    state.q_decisions[r.key]["limit"] = float(state[_wkey("limit", r)])


def _chosen(rules: list[service.Rule]) -> list[service.Rule]:
    """Regras marcadas, já com o limiar editado na tela."""
    chosen = []
    for r in rules:
        decision = state.q_decisions.get(r.key)
        if decision and decision["apply"]:
            rule = copy.copy(r)
            rule.threshold = decision["limit"]
            chosen.append(rule)
    return chosen


def _clear_filters() -> None:
    state.qf_q, state.qf_origin, state.qf_alarm = "", [], False


def _visible(r: service.Rule) -> bool:
    query = state.get("qf_q", "").strip().lower()
    if query and query not in f"{r.table} {r.column} {service.KINDS[r.kind]} {r.justification}".lower():
        return False
    origins = state.get("qf_origin") or []
    if origins and r.origin not in {ORIGINS[o] for o in origins}:
        return False
    if state.get("qf_alarm") and not r.false_alarm:
        return False
    return True


def _render_row(r: service.Rule, container_key: str) -> None:
    decision = state.q_decisions[r.key]
    state.setdefault(_wkey("apply", r), decision["apply"])
    state.setdefault(_wkey("limit", r), decision["limit"])
    row = st.container(key=container_key)
    c_apply, c_rule, c_origin, c_limit, c_test, c_why = row.columns(COLUMNS)
    c_apply.checkbox(f"Aplicar a regra de {r.column}", key=_wkey("apply", r), on_change=_on_apply, args=(r,),
                     label_visibility="collapsed")
    with c_rule:
        st.markdown(f"`{r.column}`")
        st.caption(service.KINDS[r.kind] + (f" · {r.params_text}" if r.params_text else ""))  # sem formatação de código: contraste
    with c_origin:
        st.badge("Gemini" if r.origin == "gemini" else "Dataplex", color="blue" if r.origin == "gemini" else "gray")
        st.badge(r.confidence.capitalize(), color=CONFIDENCE_COLOR[r.confidence])
    c_limit.number_input(f"Limiar de {r.column}", min_value=0.5, max_value=1.0, step=0.01, format="%.2f",
                         key=_wkey("limit", r), on_change=_on_limit, args=(r,), label_visibility="collapsed")
    with c_test:
        if r.false_alarm:
            st.markdown(f":red[**{r.dry_run_rate:.1%}**]")
            st.badge("Falso alarme", icon=":material/warning:", color="red")
        else:
            st.markdown(f"**{r.dry_run_rate:.1%}**")
            st.badge("Passou", icon=":material/check:", color="green")
    c_why.markdown(r.justification)


@st.fragment
def rules_review() -> None:
    """Revisão das regras propostas. Fragmento: marcar, editar o limiar ou filtrar reexecuta só esta parte."""
    rules: list[service.Rule] = state.rules
    dataset = state.get("q_dataset", "")

    f1, f2 = st.columns([2, 3], vertical_alignment="bottom")
    f1.text_input("Buscar regras", key="qf_q", placeholder="Buscar por tabela, coluna ou justificativa",
                  label_visibility="collapsed", icon=":material/search:")
    f2.pills("Origem", list(ORIGINS), selection_mode="multi", key="qf_origin", label_visibility="collapsed")
    with st.container(horizontal=True, vertical_alignment="center"):
        st.toggle("Só com falso alarme", key="qf_alarm")
        st.button("Limpar filtros", icon=":material/filter_alt_off:", on_click=_clear_filters, width="content")

    false_alarms = [r for r in rules if r.false_alarm]
    shown_rules = [r for r in rules if _visible(r)]
    chosen = _chosen(rules)
    with st.container(horizontal=True, vertical_alignment="center"):
        st.badge(f"{len(rules)} regras", color="gray")
        st.badge(f"{len(chosen)} selecionadas", color="green")
        st.badge(f"{sum(r.origin == 'gemini' for r in rules)} do Gemini", color="blue")
        st.badge(f"{sum(r.origin != 'gemini' for r in rules)} do Dataplex", color="gray")
        st.badge(f"{len(false_alarms)} com falso alarme", icon=":material/warning:", color="red" if false_alarms else "gray")
        if len(shown_rules) != len(rules):
            st.badge(f"{len(shown_rules)} visíveis com os filtros", icon=":material/filter_alt:", color="blue")
    for r in false_alarms:
        st.error(f"**`{r.table}.{r.column}`**: a regra falha até em dado limpo "
                 f"({r.dry_run_rate:.1%} de aprovação, abaixo do limiar de {r.threshold:.0%}). "
                 "Ela viria desmarcada: revise o limiar ou descarte.", icon=":material/warning:")

    if not shown_rules:
        st.info("Nenhuma regra corresponde aos filtros.", icon=":material/search_off:")

    by_table: dict[str, list[service.Rule]] = {}
    for r in shown_rules:
        by_table.setdefault(r.table, []).append(r)
    for index, (table, rows) in enumerate(by_table.items()):
        limit = ui.limit_of(f"q_{table}", PAGE)
        with st.expander(f":material/table_chart: {table}", expanded=index == 0):
            with st.container(key=f"rev_header_q{index}"):
                header = st.columns(COLUMNS)
                for column, label in zip(header, ["Aplicar", "Regra", "Origem", "Limiar", "Teste em dado limpo", "Justificativa"]):
                    column.caption(f"**{label}**")
            for n, r in enumerate(rows[:limit]):
                _render_row(r, f"rev_row_q{state.rules_gen}_{index}_{n}")
            ui.show_more(f"q_{table}", len(rows), min(limit, len(rows)), PAGE)

    if st.button(f"Aprovar {len(chosen)} regras", type="primary", icon=":material/rule:", disabled=not chosen, width="content"):
        state.approved_rules = chosen
        state.publish_confirm = False
        audit.record(state.audit, "Aprovar regras", f"{len(chosen)} regras em {dataset}", batch=uuid.uuid4().hex[:8])
        ui.persist()
        st.toast(f"{len(chosen)} regras aprovadas. Siga para a aba 3.", icon=":material/check_circle:")
        st.rerun()


# ---------------------------------------------------------------- página

ui.page_header(
    "Data Quality",
    "Profiling do dado, regras propostas pela IA e testadas antes de publicar. "
    "As regras viram código (YAML) e só viram scans depois da sua aprovação.",
)

c1, c2 = st.columns([1.3, 3.7])
dataset = c1.selectbox("Dataset", mock.list_datasets())
state.q_dataset = dataset
available = mock.list_tables(dataset)
tables = c2.multiselect("Tabelas", available, default=available)

profiled = bool(tables) and all((dataset, t) in state.profiles for t in tables)
rules: list[service.Rule] = state.rules
approved: list[service.Rule] = state.approved_rules
published: list[service.Rule] = state.published_rules

ui.stepper([
    ("Profiling", "done" if profiled else "current"),
    ("Regras", "todo" if not profiled else ("done" if approved else "current")),
    ("Publicação", "todo" if not approved else ("done" if published else "current")),
])

tab_profile, tab_rules, tab_publish = st.tabs(["1. Profiling", "2. Regras", "3. Publicação e resultados"])

# ---------- 1. Profiling ----------
with tab_profile:
    st.caption("O profiling mostra como o dado realmente se comporta: nulos, valores distintos e faixas.")
    if st.button("Executar profiling", type="primary", icon=":material/query_stats:", disabled=not tables):
        profiles, failed = {}, False
        progress = st.progress(0.0, text="Executando profiling...")
        for i, table in enumerate(tables, 1):
            ok, frame = ui.run(f"Profiling de {table}", service.profile, dataset, table)
            if not ok:
                failed = True
                break
            profiles[(dataset, table)] = frame
            progress.progress(i / len(tables), text=f"Profiling de {table} ({i}/{len(tables)})")
        progress.empty()
        if not failed:   # só atualiza o estado se todas as tabelas deram certo
            state.profiles.update(profiles)
            st.rerun()
    shown = [t for t in tables if (dataset, t) in state.profiles]
    for table in shown:
        with st.expander(f":material/table_chart: {table}", expanded=len(shown) == 1):
            st.dataframe(state.profiles[(dataset, table)], hide_index=True, column_config={
                "% nulos": st.column_config.ProgressColumn("% nulos", min_value=0, max_value=100, format="%.1f%%")})
    if not shown:
        st.info("Nenhum profiling executado ainda. Ele é o primeiro passo: as regras partem do que o dado mostra.",
                icon=":material/info:")

# ---------- 2. Regras ----------
with tab_rules:
    st.caption("O Gemini propõe regras com base no profiling e na amostra, e o portal as compara com as recomendações "
               "do Dataplex. Cada regra é testada em **dado limpo** antes de ser publicada.")
    if st.button("Propor regras", type="primary", icon=":material/auto_awesome:", disabled=not profiled):
        with st.spinner("Propondo e testando regras..."):
            ok, proposed = ui.run("Propor regras", service.propose, dataset, tables)
        if ok:
            state.rules = proposed
            state.rules_gen += 1
            state.q_decisions = {r.key: {"apply": not r.false_alarm and r.confidence != "baixa", "limit": float(r.threshold)}
                                 for r in proposed}
            state.approved_rules = []
            state.publish_confirm = False
            state.limits = {k: v for k, v in state.limits.items() if not k.startswith("q_")}
            ui.persist()
            st.rerun()
    if not profiled:
        st.info("Execute o profiling das tabelas escolhidas (etapa 1) para liberar a proposta de regras.",
                icon=":material/lock:")
    elif rules:
        rules_review()
    else:
        st.info("Clique em **Propor regras** para gerar e testar as regras.", icon=":material/info:")

# ---------- 3. Publicação e resultados ----------
with tab_publish:
    if published:
        st.header("Resultados dos scans", anchor=False)
        st.caption("O dado atual tem defeitos injetados de propósito (nulos, formatos, faixas, duplicidades). "
                   "As violações são o que as regras conseguem pegar.")
        results = pd.DataFrame([{
            "Regra": f"{r.table}.{r.column} ({service.KINDS[r.kind]})",
            "Taxa de aprovação": round(service.scan(r) * 100, 1),
            "Limiar": round(r.threshold * 100, 1),
        } for r in published])
        results["Status"] = ["OK" if a >= b else "Violação" for a, b in zip(results["Taxa de aprovação"], results["Limiar"])]

        violations = int((results["Status"] == "Violação").sum())
        ui.kpis([
            {"label": "Scans publicados", "value": len(results)},
            {"label": "Violações detectadas", "value": violations},
            {"label": "Regras sem violação", "value": len(results) - violations},
        ])

        def _tint(value: str) -> str:
            return "background-color: rgba(179,38,30,0.16)" if value == "Violação" else "background-color: rgba(10,92,32,0.14)"

        st.dataframe(
            results.sort_values("Status", ascending=False).style.map(_tint, subset=["Status"]),
            hide_index=True,
            column_config={
                "Regra": st.column_config.TextColumn(width="large"),
                "Taxa de aprovação": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%"),
                "Limiar": st.column_config.NumberColumn(format="%.0f%%"),
            },
        )
        st.divider()

    if not approved:
        st.info("Nenhuma regra aprovada. Aprove as regras na etapa 2 para publicá-las.", icon=":material/lock:")
    else:
        tables_n = len({r.table for r in approved})
        st.markdown(f"**{len(approved)} regras aprovadas** em {tables_n} tabelas, prontas para virar scans.")
        with st.container(horizontal=True):
            st.button(("Republicar" if published else "Publicar") + f" {len(approved)} scans", type="primary",
                      icon=":material/rocket_launch:", on_click=lambda: state.update(publish_confirm=True),
                      disabled=state.publish_confirm, width="content")
            st.download_button("Baixar YAML", service.to_yaml(approved), file_name=f"regras_{dataset}.yaml",
                               mime="text/yaml", icon=":material/download:", width="content")

        if state.publish_confirm:
            decision = ui.confirm_panel(
                "Confirmar publicação",
                [f"**{len(approved)}** scans serão criados no Dataplex, com execução agendada.",
                 "Cada scan usa o limiar aprovado; violações geram alerta e ficam em uma tabela do BigQuery."],
                f"Publicar {len(approved)} scans", key="publish",
            )
            if decision is True:
                ok, _ = ui.run("Publicar scans", lambda: None)
                if ok:
                    state.published_rules = approved
                    state.publish_confirm = False
                    audit.record(state.audit, "Publicar scans", f"{len(approved)} scans em {dataset}", batch=uuid.uuid4().hex[:8])
                    ui.persist()
                    st.toast("Scans publicados (simulado) com execução agendada.", icon=":material/check_circle:")
                    st.rerun()
            elif decision is False:
                state.publish_confirm = False
                st.rerun()

        with st.expander("Regras como código (YAML)", icon=":material/code:"):
            st.code(service.to_yaml(approved), language="yaml")
