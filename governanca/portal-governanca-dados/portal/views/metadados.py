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
state.setdefault("meta_decisions", {})   # chave da sugestão -> {"apply": bool, "text": str}: fonte da verdade da revisão

CONFIDENCE_COLOR = {"alta": "green", "média": "orange", "baixa": "red"}
COLUMNS = [0.7, 2.2, 2.6, 4.4, 1.4]  # Aplicar · Coluna · Existente · Proposto · Confiança
PAGE = 6                             # linhas por tabela antes de "Mostrar mais"

# As decisões da pessoa (marcar, editar o texto) ficam em `meta_decisions`, e não nos widgets: o Streamlit descarta
# o estado de widgets que não são desenhados, e com busca, filtros e "mostrar mais" nem toda linha existe na tela.


def _wkey(kind: str, s: service.Suggestion) -> str:
    return f"m_{kind}_{state.meta_gen}_{s.key}"


def _on_apply(s: service.Suggestion) -> None:
    state.meta_decisions[s.key]["apply"] = bool(state[_wkey("apply", s)])


def _on_text(s: service.Suggestion) -> None:
    state.meta_decisions[s.key]["text"] = str(state[_wkey("text", s)])


def _chosen(items: list[service.Suggestion]) -> list[tuple[service.Suggestion, str]]:
    """O que será gravado: itens marcados que têm texto, com a edição feita na tela."""
    chosen = []
    for s in items:
        decision = state.meta_decisions.get(s.key)
        if decision and decision["apply"] and decision["text"].strip():
            chosen.append((s, decision["text"].strip()))
    return chosen


def _set_selection(rule) -> None:
    for s in state.suggestions:
        state.meta_decisions[s.key]["apply"] = rule(s)
        state.pop(_wkey("apply", s), None)  # o checkbox volta a ler o valor novo


def _select_high_confidence() -> None:
    _set_selection(lambda s: s.confidence == "alta" and not s.has_human_text and bool(s.proposed.strip()))


def _clear_selection() -> None:
    _set_selection(lambda s: False)


def _clear_filters() -> None:
    state.mf_q, state.mf_conf = "", []
    state.mf_pending = state.mf_human = state.mf_sens = False


def _visible(s: service.Suggestion) -> bool:
    query = state.get("mf_q", "").strip().lower()
    if query and query not in f"{s.table} {s.column or ''} {s.proposed} {s.existing} {s.classification}".lower():
        return False
    if state.get("mf_conf") and s.confidence not in state.mf_conf:
        return False
    if state.get("mf_human") and not s.has_human_text:
        return False
    if state.get("mf_sens") and not s.sensitive:
        return False
    if state.get("mf_pending") and state.applied_meta.get(s.key):
        return False
    return True


def _render_row(s: service.Suggestion, column_type: str, container_key: str) -> None:
    decision = state.meta_decisions[s.key]
    state.setdefault(_wkey("apply", s), decision["apply"])
    state.setdefault(_wkey("text", s), decision["text"])
    row = st.container(key=container_key)
    c_apply, c_col, c_exist, c_prop, c_conf = row.columns(COLUMNS)
    c_apply.checkbox(f"Aplicar em {s.column or s.table}", key=_wkey("apply", s), on_change=_on_apply, args=(s,),
                     label_visibility="collapsed")
    with c_col:
        st.markdown(f"`{s.column or 'tabela'}`")
        if s.column:
            st.caption(column_type)
        if s.sensitive:
            st.badge("Sensível", icon=":material/lock:", color="red")
    with c_exist:
        if s.has_human_text:
            st.markdown(s.existing)
            st.badge("Escrito por uma pessoa", color="orange")
        else:
            st.caption("Sem descrição")
        if state.applied_meta.get(s.key):
            st.badge("Gravada", icon=":material/check:", color="green")
    with c_prop:
        st.text_area(f"Descrição proposta para {s.column or s.table}", key=_wkey("text", s), on_change=_on_text, args=(s,),
                     height=68, label_visibility="collapsed",
                     placeholder="Sem proposta: escreva a descrição ou deixe sem aplicar.")
        st.caption(f":material/fact_check: {s.evidence}")
    with c_conf:
        st.badge(s.confidence.capitalize(), color=CONFIDENCE_COLOR[s.confidence])
        st.caption(s.classification)


def _write(chosen: list[tuple[service.Suggestion, str]]) -> str:
    """Grava as descrições aprovadas e devolve o id do lote. Só altera o estado depois de tudo validado."""
    batch = uuid.uuid4().hex[:8]
    for s, text in chosen:
        before = state.applied_meta.get(s.key, s.existing)
        state.applied_meta[s.key] = text
        audit.record(state.audit, "Gravar descrição", s.key, before=before, after=text, batch=batch)
    return batch


def _undo_last_batch() -> bool:
    batch = audit.last_open_batch(state.audit, "Gravar descrição")
    if batch is None:
        return False
    for entry in reversed(state.audit):
        if entry.batch == batch and entry.action == "Gravar descrição" and not entry.undone:
            if entry.before:
                state.applied_meta[entry.target] = entry.before
            else:
                state.applied_meta.pop(entry.target, None)
            entry.undone = True
    audit.record(state.audit, "Desfazer aprovação", f"lote {batch}")
    return True


@st.fragment
def review() -> None:
    """Revisão das sugestões. É um fragmento: marcar, editar ou filtrar reexecuta só esta parte, não a página inteira."""
    items: list[service.Suggestion] = state.suggestions

    if flash := state.pop("meta_flash", None):
        st.success(flash, icon=":material/check_circle:")

    st.header("Revisão", anchor=False)
    f1, f2 = st.columns([2, 3], vertical_alignment="bottom")
    f1.text_input("Buscar", key="mf_q", placeholder="Buscar por tabela, coluna ou texto", label_visibility="collapsed",
                  icon=":material/search:")
    f2.pills("Confiança", ["alta", "média", "baixa"], selection_mode="multi", key="mf_conf", label_visibility="collapsed",
             format_func=str.capitalize)
    with st.container(horizontal=True, vertical_alignment="center"):
        st.toggle("Só pendentes", key="mf_pending", help="Esconde o que já foi gravado pelo portal.")
        st.toggle("Texto de uma pessoa", key="mf_human")
        st.toggle("Sensíveis", key="mf_sens")
        st.button("Limpar filtros", icon=":material/filter_alt_off:", on_click=_clear_filters, width="content")

    shown_items = [s for s in items if _visible(s)]
    chosen = _chosen(items)
    with st.container(horizontal=True, vertical_alignment="center"):
        st.badge(f"{len(items)} sugestões", color="gray")
        st.badge(f"{len(chosen)} selecionadas", color="green")
        st.badge(f"{sum(s.has_human_text for s in items)} com texto de uma pessoa", color="orange")
        low = sum(s.confidence == "baixa" for s in items)
        st.badge(f"{low} de baixa confiança", color="red" if low else "gray")
        if len(shown_items) != len(items):
            st.badge(f"{len(shown_items)} visíveis com os filtros", icon=":material/filter_alt:", color="blue")
    st.caption("Descrições escritas por pessoas e itens de baixa confiança vêm **desmarcados**: marque *Aplicar* para "
               "confirmar a troca. O texto proposto pode ser editado na própria linha.")

    with st.container(horizontal=True):
        st.button("Selecionar só confiança alta", icon=":material/done_all:", on_click=_select_high_confidence, width="content")
        st.button("Limpar seleção", icon=":material/deselect:", on_click=_clear_selection, width="content")

    if not shown_items:
        st.info("Nenhum item corresponde aos filtros.", icon=":material/search_off:")

    types = {(s.table, name): kind for s in items if s.column is None for name, kind, _ in service.schema_of(s.dataset, s.table)}
    by_table: dict[str, list[service.Suggestion]] = {}
    for s in shown_items:
        by_table.setdefault(s.table, []).append(s)

    for index, (table, rows) in enumerate(by_table.items()):
        limit = ui.limit_of(f"m_{table}", PAGE)
        with st.expander(f":material/table_chart: {table}", expanded=index == 0):
            selected = sum(state.meta_decisions[s.key]["apply"] for s in rows)
            st.caption(f"{selected} de {len(rows)} selecionadas")
            with st.container(key=f"rev_header_{index}"):
                header = st.columns(COLUMNS)
                for column, label in zip(header, ["Aplicar", "Coluna", "Existente", "Proposto (editável)", "Confiança"]):
                    column.caption(f"**{label}**")
            for n, s in enumerate(rows[:limit]):
                _render_row(s, types.get((s.table, s.column), ""), f"rev_row_{state.meta_gen}_{index}_{n}")
            ui.show_more(f"m_{table}", len(rows), min(limit, len(rows)), PAGE)

    # ---------- ação e confirmação ----------
    with st.container(horizontal=True):
        review_clicked = st.button(f"Revisar e gravar ({len(chosen)})", type="primary", icon=":material/rule:",
                                   disabled=not chosen or state.meta_confirm, key="meta_review", width="content")
        undo_clicked = st.button("Desfazer última aprovação", icon=":material/undo:", width="content")

    if review_clicked:
        state.meta_confirm = True
        st.rerun()  # atualiza também o indicador de etapas, que fica fora do fragmento

    if undo_clicked:
        if _undo_last_batch():
            ui.persist()
            state.meta_flash = "Última aprovação desfeita."
            st.rerun()
        st.info("Não há aprovação para desfazer.", icon=":material/info:")

    if state.meta_confirm and chosen:
        overwrites = [s for s, _ in chosen if s.has_human_text]
        sensitive = sum(s.sensitive for s, _ in chosen)
        lines = [
            f"**{len(chosen)}** descrições serão gravadas em **{len({s.table for s, _ in chosen})}** tabelas.",
            f"**{len(items) - len(chosen)}** itens ficam como estão (não marcados ou sem texto).",
        ]
        if sensitive:
            lines.append(f"**{ui.plural(sensitive, 'coluna está marcada', 'colunas estão marcadas')}** como sensível: "
                         "a classificação ainda precisa de confirmação.")
        warnings = []
        if overwrites:
            names = ", ".join(f"`{s.table}.{s.column or 'tabela'}`" for s in overwrites)
            warnings.append(f"{len(overwrites)} descrição(ões) escrita(s) por pessoas serão substituídas: {names}. "
                            "O texto anterior fica na auditoria e pode ser restaurado.")
        decision = ui.confirm_panel("Confirmar gravação", lines, f"Gravar {len(chosen)} descrições", warnings, key="meta")
        if decision is True:
            ok, _ = ui.run("Gravar descrições", _write, chosen)
            if ok:
                ui.persist()
                state.meta_confirm = False
                state.meta_flash = (f"{len(chosen)} descrições gravadas (simulado) e registradas na auditoria. "
                                    "Elas já aparecem nas respostas do Catálogo.")
                st.rerun()
        elif decision is False:
            state.meta_confirm = False
            st.rerun()


# ---------------------------------------------------------------- página

ui.page_header(
    "Metadados",
    "O modelo propõe descrições e classificações a partir do contexto do dado. "
    "Você revisa, edita e aprova; nada é gravado sem a sua confirmação.",
)

items: list[service.Suggestion] = state.suggestions
has_items = bool(items)
ui.stepper([
    ("Escolher tabelas", "done" if has_items else "current"),
    ("Revisar sugestões", "todo" if not has_items else ("done" if state.meta_confirm else "current")),
    ("Confirmar e gravar", "current" if state.meta_confirm else "todo"),
])

c1, c2, c3 = st.columns([1.3, 2.7, 1], vertical_alignment="bottom")
dataset = c1.selectbox("Dataset", service.list_datasets())
available = service.list_tables(dataset)
tables = c2.multiselect("Tabelas", available, default=available)
generate = c3.button("Gerar sugestões", type="primary", icon=":material/auto_awesome:", disabled=not tables, width="stretch")

if tables:
    with st.expander("O que o modelo recebe (esquema e amostra mascarada)", icon=":material/visibility:"):
        table = st.selectbox("Tabela", tables, key="contexto_tabela")
        schema = pd.DataFrame(service.schema_of(dataset, table), columns=["Coluna", "Tipo", "Descrição existente"])
        st.markdown("**Esquema e metadados existentes**")
        st.dataframe(schema, hide_index=True)
        st.markdown(f"**Amostra** (até {settings.max_sample_rows} linhas, dados sensíveis mascarados)")
        st.dataframe(masking.mask_dataframe(mock.get_sample(dataset, table, dirty=True)).head(settings.max_sample_rows),
                     hide_index=True)

if generate:
    with st.spinner("Montando contexto e gerando sugestões..."):
        ok, generated = ui.run("Gerar sugestões", service.generate, dataset, tables)
    if ok:
        state.suggestions = generated
        state.meta_decisions = {s.key: {"apply": s.apply_by_default, "text": s.proposed} for s in generated}
        state.meta_gen += 1
        state.meta_confirm = False
        state.limits = {k: v for k, v in state.limits.items() if not k.startswith("m_")}
        st.rerun()

if not has_items:
    st.info("Escolha as tabelas e clique em **Gerar sugestões** para começar a revisão.", icon=":material/info:")
else:
    review()

history = [e for e in state.audit if e.action == "Gravar descrição"]
if history:
    with st.expander(f"Auditoria de gravações ({len(history)})", icon=":material/history:"):
        st.dataframe(pd.DataFrame([{
            "Quando": e.at, "Quem": e.user, "Objeto": e.target, "Antes": e.before, "Depois": e.after,
            "Desfeito": "sim" if e.undone else "",
        } for e in reversed(history)]), hide_index=True)
