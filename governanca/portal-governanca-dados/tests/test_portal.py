"""Testes de fumaça da interface: rodam o app inteiro em memória, sem navegador."""
from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parent.parent / "portal" / "app.py")


def open_page(page: str | None = None) -> AppTest:
    at = AppTest.from_file(APP, default_timeout=60).run()
    assert not at.exception, at.exception
    if page:
        at.switch_page(page).run()
        assert not at.exception, at.exception
    return at


def click(at: AppTest, label_part: str) -> AppTest:
    button = next(b for b in at.button if label_part in b.label)
    return button.click().run()


def click_key(at: AppTest, key: str) -> AppTest:
    return next(b for b in at.button if b.key == key).click().run()


def button(at: AppTest, label_part: str):
    return next(b for b in at.button if label_part in b.label)


def approve_default_metadata() -> AppTest:
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    at = click_key(at, "meta_review")
    return click_key(at, "meta_ok")


# ---------- páginas ----------

def test_home_loads_with_governance_metrics():
    at = open_page()
    assert "Portal de Governança de Dados" in at.title[0].value
    assert [m.label for m in at.metric] == [
        "Cobertura", "A documentar", "Gravadas pelo portal", "Scans ativos",
    ]


def test_every_page_renders_without_error():
    for page in ("views/metadados.py", "views/qualidade.py", "views/catalogo.py", "views/configuracoes.py"):
        at = open_page(page)
        assert at.title, page


# ---------- metadados ----------

def test_metadata_requires_confirmation_before_writing():
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    assert at.session_state.suggestions

    at = click_key(at, "meta_review")
    assert at.session_state.meta_confirm
    assert not at.session_state.applied_meta, "ainda não pode ter gravado nada: falta confirmar"

    at = click_key(at, "meta_back")
    assert not at.session_state.meta_confirm
    assert not at.session_state.applied_meta


def test_metadata_confirm_writes_only_safe_items_and_undo_restores():
    at = approve_default_metadata()
    applied = at.session_state.applied_meta
    assert len(applied) == 21
    # texto escrito por pessoa e itens de baixa confiança não são sobrescritos por padrão
    assert "combustiveis_gold.fct_preco_combustivel.valor_venda" not in applied
    assert "combustiveis_gold.fct_preco_combustivel.*" not in applied
    assert any(e.action == "Gravar descrição" for e in at.session_state.audit)

    at = click(at, "Desfazer última aprovação")
    assert not at.session_state.applied_meta
    assert all(e.undone for e in at.session_state.audit if e.action == "Gravar descrição")


def test_overwriting_human_text_is_called_out_in_confirmation():
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    human = next(s for s in at.session_state.suggestions if s.has_human_text)
    key = f"m_apply_{at.session_state.meta_gen}_{human.key}"
    at.checkbox(key=key).set_value(True).run()
    at = click_key(at, "meta_review")
    assert any("escrita(s) por pessoas" in w.value for w in at.warning), "a substituição de texto humano precisa de aviso"
    at = click_key(at, "meta_ok")
    assert human.key in at.session_state.applied_meta


def test_clear_selection_disables_the_review_button():
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    at = click(at, "Limpar seleção")
    assert next(b for b in at.button if b.key == "meta_review").disabled


# ---------- data quality ----------

def test_quality_steps_are_gated_and_flow_runs_end_to_end():
    at = open_page("views/qualidade.py")
    assert button(at, "Propor regras").disabled, "só se propõe regra depois do profiling"

    at = click(at, "Executar profiling")
    assert at.session_state.profiles
    assert not button(at, "Propor regras").disabled

    at = click(at, "Propor regras")
    rules = at.session_state.rules
    assert rules and any(r.false_alarm for r in rules)

    at = click(at, "Aprovar ")
    approved = at.session_state.approved_rules
    assert approved
    assert not any(r.false_alarm for r in approved), "regra com falso alarme não deve vir pré-aprovada"

    at = click(at, "Publicar ")
    assert not at.session_state.published_rules, "publicar também exige confirmação"
    at = click_key(at, "publish_ok")
    assert at.session_state.published_rules
    assert any("Violação" in str(df.value) for df in at.dataframe), "os defeitos injetados deveriam gerar violações"


# ---------- catálogo ----------

def test_catalog_answers_and_abstains():
    at = open_page("views/catalogo.py")
    at.chat_input[0].set_value("O que significa o valor de compra?").run()
    at.chat_input[0].set_value("Qual a cotação do dólar hoje?").run()
    assert not at.exception

    answers = [m["answer"] for m in at.session_state.chat if m["role"] == "assistant"]
    assert not answers[0].abstained and answers[0].citations
    assert answers[1].abstained


def test_metadata_written_in_portal_reaches_catalog():
    at = approve_default_metadata()
    at = at.switch_page("views/catalogo.py").run()
    at.chat_input[0].set_value("O que é a chave substituta do posto?").run()
    answer = next(m["answer"] for m in at.session_state.chat if m["role"] == "assistant")
    assert not answer.abstained
    assert answer.citations[0][0].source == "Metadados (gravados no portal)"


# ---------- persistência ----------

def test_state_survives_a_page_reload():
    first = approve_default_metadata()
    assert first.session_state.applied_meta

    reloaded = open_page()   # nova sessão do navegador: o estado em memória começa vazio
    assert reloaded.session_state.applied_meta == first.session_state.applied_meta
    assert len(reloaded.session_state.audit) == len(first.session_state.audit)


def test_reset_demo_clears_persisted_state(isolated_state):
    approve_default_metadata()
    assert isolated_state.exists()

    at = open_page("views/configuracoes.py")
    at = click(at, "Restaurar demonstração")
    assert not at.session_state.applied_meta
    assert not isolated_state.exists()


# ---------- revisão em escala: busca, filtros e "mostrar mais" ----------

def test_filters_and_show_more_never_lose_the_reviewers_edits():
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    first = next(s for s in at.session_state.suggestions if s.column == "sk_posto" and s.table == "fct_preco_combustivel")
    text_key = f"m_text_{at.session_state.meta_gen}_{first.key}"

    at.text_area(key=text_key).set_value("Texto editado pela pessoa").run()
    assert at.session_state.meta_decisions[first.key]["text"] == "Texto editado pela pessoa"

    # um filtro que esconde a linha editada: o widget some da tela, mas a edição tem de sobreviver
    at.text_input(key="mf_q").set_value("dim_localidade").run()
    assert not any(t.key == text_key for t in at.text_area)
    assert at.session_state.meta_decisions[first.key]["text"] == "Texto editado pela pessoa"

    # tira o filtro: a linha volta com o texto editado
    at.text_input(key="mf_q").set_value("").run()
    assert at.text_area(key=text_key).value == "Texto editado pela pessoa"


def test_show_more_reveals_the_rest_of_a_long_table():
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    first_table = at.session_state.suggestions[0].table
    total = sum(s.table == first_table for s in at.session_state.suggestions)
    assert total > 6, "a tabela de exemplo precisa ter mais linhas que uma página"

    def rows_of_first_table():
        return [t for t in at.text_area if f".{first_table}." in t.key]

    assert len(rows_of_first_table()) == 6
    at = click_key(at, f"more_m_{first_table}")
    assert len(rows_of_first_table()) == total


def test_confidence_filter_hides_other_levels():
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    at.text_input(key="mf_q").set_value("sk_").run()
    visible = [t.key for t in at.text_area]
    assert visible and all("sk_" in key for key in visible)


# ---------- erros: o portal avisa, orienta e não deixa nada pela metade ----------

def set_fault(at: AppTest, fault: str) -> AppTest:
    at.session_state.settings.simulated_fault = fault
    return at.run()


def test_failure_while_generating_shows_actionable_error_and_keeps_state():
    at = open_page("views/metadados.py")
    at = set_fault(at, "Permissão negada")
    at = click(at, "Gerar sugestões")
    assert not at.exception
    assert any("Sem permissão" in e.value and "O que fazer" in e.value for e in at.error)
    assert not at.session_state.suggestions


def test_failure_while_writing_leaves_nothing_half_written():
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    at = click_key(at, "meta_review")
    at = set_fault(at, "Tempo esgotado")
    at = click_key(at, "meta_ok")
    assert any("demorou demais" in e.value for e in at.error)
    assert not at.session_state.applied_meta
    assert not at.session_state.audit
    assert at.session_state.meta_confirm, "a confirmação continua aberta para tentar de novo"

    at = set_fault(at, "Nenhuma")
    at = click_key(at, "meta_ok")
    assert len(at.session_state.applied_meta) == 21


def test_unexpected_exception_becomes_a_friendly_message_not_a_broken_page():
    at = open_page("views/qualidade.py")
    at = set_fault(at, "Erro inesperado")
    at = click(at, "Executar profiling")
    assert not at.exception
    assert any("Não foi possível concluir a operação" in e.value for e in at.error)
    assert not at.session_state.profiles


def test_catalog_failure_appears_inside_the_conversation():
    at = open_page("views/catalogo.py")
    at = set_fault(at, "Cota excedida")
    at.chat_input[0].set_value("O que significa o valor de compra?").run()
    assert not at.exception
    assert any("Limite de uso atingido" in e.value for e in at.error)


def test_losing_the_ability_to_save_is_reported(monkeypatch, tmp_path):
    blocker = tmp_path / "arquivo_no_caminho"
    blocker.write_text("x")
    monkeypatch.setenv("GOVERNANCA_STATE_FILE", str(blocker / "estado.json"))   # o "diretório" é um arquivo: não dá para gravar
    at = approve_default_metadata()
    assert not at.exception
    assert at.session_state.applied_meta, "a gravação em memória continua valendo"
    assert at.session_state.persist_failed
    assert any("não está sendo salvo" in w.value for w in at.sidebar.warning)
