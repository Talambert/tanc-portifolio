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


def test_home_loads_with_simulation_banner():
    at = open_page()
    assert "Portal de Governança de Dados" in at.title[0].value
    assert any("Modo simulação" in i.value for i in at.info)
    assert [m.label for m in at.metric] == ["Datasets", "Tabelas catalogadas", "Descrições gravadas", "Scans de qualidade"]


def test_every_page_renders_without_error():
    for page in ("views/metadados.py", "views/qualidade.py", "views/catalogo.py", "views/configuracoes.py"):
        at = open_page(page)
        assert at.title, page


def test_metadata_flow_generate_approve_undo():
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    assert at.session_state.suggestions

    at = click(at, "Aprovar e gravar")
    applied = at.session_state.applied_meta
    assert applied, "a aprovação deveria gravar as sugestões pré-selecionadas"
    # texto humano e baixa confiança não são sobrescritos por padrão
    assert "combustiveis_gold.fct_preco_combustivel.valor_venda" not in applied
    assert "combustiveis_gold.fct_preco_combustivel.*" not in applied
    assert any(e.action == "Gravar descrição" for e in at.session_state.audit)

    at = click(at, "Desfazer última aprovação")
    assert not at.session_state.applied_meta
    assert all(e.undone for e in at.session_state.audit if e.action == "Gravar descrição")


def test_quality_flow_profile_propose_approve_publish():
    at = open_page("views/qualidade.py")
    at = click(at, "Executar profiling")
    assert at.session_state.profiles

    at = click(at, "Propor regras")
    rules = at.session_state.rules
    assert rules and any(r.false_alarm for r in rules)

    at = click(at, "Aprovar regras selecionadas")
    approved = at.session_state.approved_rules
    assert approved
    assert not any(r.false_alarm for r in approved), "regra com falso alarme não deve vir pré-aprovada"

    at = click(at, "Publicar scans")
    assert at.session_state.published_rules
    assert any("Violação" in str(df.value) for df in at.dataframe), "os defeitos injetados deveriam gerar violações"


def test_catalog_answers_and_abstains():
    at = open_page("views/catalogo.py")
    at.chat_input[0].set_value("O que significa o valor de compra?").run()
    at.chat_input[0].set_value("Qual a cotação do dólar hoje?").run()
    assert not at.exception

    answers = [m["answer"] for m in at.session_state.chat if m["role"] == "assistant"]
    assert not answers[0].abstained and answers[0].citations
    assert answers[1].abstained


def test_metadata_written_in_portal_reaches_catalog():
    at = open_page("views/metadados.py")
    at = click(at, "Gerar sugestões")
    at = click(at, "Aprovar e gravar")
    at = at.switch_page("views/catalogo.py").run()
    assert not at.exception
    assert any("vindos do módulo de Metadados" in c.value and " 0 vindos" not in c.value for c in at.caption)
