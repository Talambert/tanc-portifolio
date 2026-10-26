"""Elementos de interface compartilhados pelas páginas do portal."""
from dataclasses import asdict

import streamlit as st

from governanca.catalogo.service import Answer
from governanca.core import errors, store
from governanca.core.audit import AuditEntry
from governanca.core.config import Settings
from governanca.qualidade.service import Rule, rule_from_dict, rule_to_dict


def init() -> None:
    """Estado da sessão. Na simulação, o que importa (auditoria, gravações e scans) é lido do arquivo local."""
    state = st.session_state
    if "settings" in state:
        return

    saved = store.load()
    state.settings = Settings()
    state.audit = [AuditEntry(**d) for d in saved.get("audit", [])]
    state.applied_meta = dict(saved.get("applied_meta", {}))   # "dataset.tabela.coluna" -> texto gravado
    state.approved_rules = [rule_from_dict(d) for d in saved.get("approved_rules", [])]
    state.published_rules = [rule_from_dict(d) for d in saved.get("published_rules", [])]

    state.suggestions = []
    state.meta_gen = 0          # muda a cada geração, para reiniciar os campos de revisão
    state.meta_confirm = False
    state.profiles = {}
    state.rules = []
    state.rules_gen = 0
    state.publish_confirm = False
    state.chat = []
    state.limits = {}           # quantos itens de cada tabela já foram mostrados ("mostrar mais")
    state.persist_failed = False


def persist() -> None:
    """Salva o que deve sobreviver a um recarregamento da página."""
    state = st.session_state
    saved = store.save({
        "audit": [asdict(e) for e in state.audit],
        "applied_meta": state.applied_meta,
        "approved_rules": [rule_to_dict(r) for r in state.approved_rules],
        "published_rules": [rule_to_dict(r) for r in state.published_rules],
    })
    state.persist_failed = not saved
    if not saved:
        st.toast("Não foi possível salvar o estado. As alterações valem nesta sessão, mas somem ao recarregar a página.",
                 icon=":material/warning:")


def reset_demo() -> None:
    """Volta à demonstração limpa: apaga o arquivo e zera a sessão."""
    store.reset()
    for key in list(st.session_state.keys()):
        del st.session_state[key]


# ---------- cabeçalho, modo e etapas ----------

_CSS = """
<style>
/* Em telas estreitas as colunas empilham: o cabeçalho da revisão perde o sentido, e cada linha precisa de separação. */
@media (max-width: 768px) {
  [class*="st-key-rev_header_"] { display: none; }
  [class*="st-key-rev_row_"] { border-top: 1px solid rgba(128, 128, 128, 0.35); padding-top: 0.75rem; margin-top: 0.25rem; }
}
</style>
"""


def inject_css() -> None:
    st.html(_CSS)


def plural(n: int, singular: str, plural_form: str) -> str:
    return f"{n} {singular if n == 1 else plural_form}"


def mode_badge() -> None:
    """Selo discreto do modo de execução, no lugar do aviso grande repetido em toda página."""
    if st.session_state.settings.simulation:
        st.badge("Simulação", icon=":material/science:", color="orange")
    else:
        st.badge("Ambiente real", icon=":material/cloud:", color="green")


def page_header(title: str, subtitle: str = "") -> None:
    left, right = st.columns([6, 1], vertical_alignment="center")
    left.title(title, anchor=False)
    with right.container(horizontal_alignment="right"):
        mode_badge()
    if subtitle:
        st.caption(subtitle)


def stepper(steps: list[tuple[str, str]]) -> None:
    """Indicador de etapas. Cada item é (rótulo, estado), com estado em {"done", "current", "todo"}."""
    parts = []
    for label, status in steps:
        if status == "done":
            parts.append(f":green[:material/check_circle:] :green[{label}]")
        elif status == "current":
            parts.append(f":material/play_circle: **{label}**")
        else:
            parts.append(f":gray[:material/radio_button_unchecked:] :gray[{label}]")
    st.markdown("  &nbsp;:gray[›]&nbsp;  ".join(parts))


def confirm_panel(title: str, lines: list[str], confirm_label: str, warnings: list[str] | None = None,
                  key: str = "confirm") -> bool | None:
    """Painel de confirmação em linha. Devolve True ao confirmar, False ao voltar e None enquanto aguarda."""
    with st.container(border=True):
        st.markdown(f"**{title}**")
        for line in lines:
            st.markdown(f"- {line}")
        for warning in warnings or []:
            st.warning(warning, icon=":material/warning:")
        with st.container(horizontal=True):
            confirmed = st.button(confirm_label, type="primary", icon=":material/check:", key=f"{key}_ok", width="content")
            cancelled = st.button("Voltar à revisão", key=f"{key}_back", width="content")
    if confirmed:
        return True
    if cancelled:
        return False
    return None


def kpis(items: list[dict]) -> None:
    """Indicadores em uma linha no desktop e em grade 2x2 no celular (o contêiner horizontal quebra linha sozinho)."""
    with st.container(horizontal=True, wrap=True, gap="medium"):
        for item in items:
            st.metric(item["label"], item["value"], delta=item.get("delta"), help=item.get("help"), width=165)


def sidebar() -> None:
    with st.sidebar:
        if st.session_state.get("persist_failed"):
            st.warning("O estado não está sendo salvo: ele some ao recarregar a página.", icon=":material/warning:")
        st.caption(":material/person: demo@local")
        st.caption("Protótipo · v0.3")


def render_answer(answer: Answer) -> None:
    """Mostra a resposta do catálogo com as fontes citadas."""
    if answer.abstained:
        st.warning(answer.text, icon=":material/help:")
        return
    st.markdown(answer.text)
    with st.expander(f"Fontes ({len(answer.citations)})", icon=":material/format_quote:"):
        for chunk, score in answer.citations:
            st.markdown(f"**{chunk.source}** · {chunk.reference}")
            st.caption(f"{chunk.text}  \nRelevância {score:.2f}")


# ---------- execução com tratamento de erro ----------

def show_error(exc: errors.GovernancaError) -> None:
    """Erro com o que aconteceu, o que fazer e um código de referência. Nunca uma tela quebrada."""
    st.error(f"**{exc.title}**\n\n{exc.what}\n\n**O que fazer:** {exc.fix}", icon=":material/error:")
    with st.expander("Detalhes técnicos", icon=":material/bug_report:"):
        st.code(f"operação: {exc.operation}\ncódigo: {exc.reference}\n{exc.detail}".strip(), language="text")


def run(operation: str, fn, *args, silent: bool = False, **kwargs) -> tuple[bool, object]:
    """Executa uma operação de serviço. Devolve (True, resultado) ou, se falhar, mostra o erro e devolve (False, None).
    Com `silent=True` não mostra nada e devolve (False, erro).

    Como o estado só é alterado por quem chama, depois de um (True, ...), uma falha nunca deixa nada pela metade.
    """
    try:
        errors.maybe_raise(st.session_state.settings.simulated_fault, operation)
        return True, fn(*args, **kwargs)
    except errors.GovernancaError as exc:
        exc.operation = exc.operation or operation
        failure = exc
    except Exception as exc:  # noqa: BLE001 - qualquer falha vira uma mensagem, nunca uma tela quebrada
        failure = errors.GovernancaError(operation, f"{type(exc).__name__}: {exc}")
    if silent:
        return False, failure   # quem chama decide onde mostrar (ex.: dentro da mensagem do chat)
    show_error(failure)
    return False, None


# ---------- listas longas ----------

def limit_of(key: str, default: int = 6) -> int:
    return st.session_state.limits.get(key, default)


def _more(key: str, step: int) -> None:
    st.session_state.limits[key] = limit_of(key) + step


def show_more(key: str, total: int, shown: int, step: int = 6) -> None:
    """Rodapé "Mostrando X de Y" com botão para ver mais itens."""
    if shown >= total:
        return
    left, right = st.columns([3, 1], vertical_alignment="center")
    left.caption(f"Mostrando {shown} de {total}")
    right.button(f"Mostrar mais {min(step, total - shown)}", key=f"more_{key}", on_click=_more, args=(key, step), width="stretch")
