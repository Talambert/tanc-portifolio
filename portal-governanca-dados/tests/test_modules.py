import pandas as pd
import yaml

from governanca.catalogo import service as catalogo
from governanca.core import store
from governanca.metadados import service as metadados
from governanca.mock import data as mock
from governanca.qualidade import service as qualidade


# ---------- Metadados ----------

def test_low_confidence_or_human_text_is_not_preselected():
    items = metadados.generate("vendas_demo", ["pedidos", "clientes"])
    by_key = {s.key: s for s in items}
    assert not by_key["vendas_demo.pedidos.status"].apply_by_default          # baixa confiança, sem proposta
    assert not by_key["vendas_demo.clientes.cpf"].apply_by_default            # já tem texto humano
    assert by_key["vendas_demo.pedidos.id_pedido"].apply_by_default


def test_sensitive_columns_are_flagged():
    items = {s.key: s for s in metadados.generate("vendas_demo", ["clientes"])}
    assert items["vendas_demo.clientes.cpf"].sensitive
    assert items["vendas_demo.clientes.email"].sensitive
    assert not items["vendas_demo.clientes.uf"].sensitive


# ---------- Qualidade ----------

def _rule(kind, column, params=None, threshold=1.0):
    return qualidade.Rule("d", "t", column, kind, params or {}, threshold, "gemini", "alta", "")


def test_evaluate_each_rule_kind():
    df = pd.DataFrame({
        "a": [1, 2, None, 4],
        "b": [1, 1, 2, 3],
        "c": [5, 50, 500, None],
        "d": ["SP", "RJ", "XX", None],
        "e": ["12.345.678/0001-90", " 12.345.678/0001-90", "x", None],
    })
    assert qualidade.evaluate(df, _rule("nao_nulo", "a")) == 0.75
    assert qualidade.evaluate(df, _rule("unico", "b")) == 0.5                      # só os valores 2 e 3 aparecem uma vez
    assert qualidade.evaluate(df, _rule("faixa", "c", {"minimo": 0, "maximo": 100})) == 2 / 3
    assert qualidade.evaluate(df, _rule("conjunto", "d", {"valores": ["SP", "RJ"]})) == 2 / 3
    assert qualidade.evaluate(df, _rule("regex", "e", {"padrao": r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}"})) == 1 / 3


def test_rules_pass_on_clean_data_except_the_known_false_alarm():
    rules = qualidade.propose("combustiveis_gold", mock.list_tables("combustiveis_gold"))
    false_alarms = [r.key for r in rules if r.false_alarm]
    assert false_alarms == ["combustiveis_gold.fct_preco_combustivel.valor_compra.nao_nulo"]


def test_rules_catch_the_injected_defects():
    """Dado atual tem defeitos injetados: as regras certas devem acusar, e as de chave limpa não."""
    rules = {r.key: r for r in qualidade.propose("combustiveis_gold", mock.list_tables("combustiveis_gold"))}
    assert qualidade.scan(rules["combustiveis_gold.dim_posto.cnpj_revenda.regex"]) < 0.99
    assert qualidade.scan(rules["combustiveis_gold.dim_localidade.uf.conjunto"]) < 1.0
    assert qualidade.scan(rules["combustiveis_gold.fct_preco_combustivel.valor_venda.faixa"]) < 0.99
    assert qualidade.scan(rules["combustiveis_gold.dim_posto.sk_posto.unico"]) == 1.0


def test_to_yaml_is_valid_and_keeps_rules():
    rules = qualidade.propose("vendas_demo", ["pedidos"])
    document = yaml.safe_load(qualidade.to_yaml(rules))
    assert document[0]["tabela"] == "vendas_demo.pedidos"
    assert len(document[0]["regras"]) == len(rules)


def test_profile_reports_nulls():
    prof = qualidade.profile("combustiveis_gold", "fct_preco_combustivel").set_index("Coluna")
    assert prof.loc["valor_compra", "% nulos"] > 80


# ---------- Catálogo ----------

def test_catalog_answers_with_citation():
    answer = catalogo.answer("O que significa o valor de compra?", catalogo.build_corpus())
    assert not answer.abstained
    assert answer.citations and "Valor de Compra" in answer.citations[0][0].text


def test_catalog_abstains_when_sources_do_not_cover():
    answer = catalogo.answer("Qual a cotação do dólar hoje?", catalogo.build_corpus())
    assert answer.abstained
    assert answer.citations == []


def test_catalog_uses_descriptions_applied_by_metadata_module():
    key = "vendas_demo.pedidos.valor_total"
    corpus = catalogo.build_corpus({key: "Valor total do pedido, em reais, já com descontos aplicados."})
    answer = catalogo.answer("valor total do pedido tem desconto?", corpus)
    assert not answer.abstained
    assert answer.citations[0][0].source == "Metadados (gravados no portal)"


# ---------- YAML, cobertura e persistência ----------

def test_yaml_keeps_value_lists_on_one_line():
    rules = qualidade.propose("combustiveis_gold", ["dim_localidade"])
    text = qualidade.to_yaml(rules)
    assert "- AC" not in text, "as 27 UFs não devem ocupar uma linha cada"
    assert "AC, AL" in text
    assert yaml.safe_load(text)[0]["regras"][0]["valores"][:2] == ["AC", "AL"]


def test_rule_roundtrip_through_dict():
    rule = qualidade.propose("vendas_demo", ["pedidos"])[1]
    assert qualidade.rule_from_dict(qualidade.rule_to_dict(rule)) == rule


def test_documentation_coverage_grows_when_descriptions_are_applied():
    before = sum(r["documentados"] for r in metadados.documentation_coverage())
    applied = {"vendas_demo.pedidos.status": "Situação do pedido.", "vendas_demo.pedidos.*": "Pedidos."}
    after = sum(r["documentados"] for r in metadados.documentation_coverage(applied))
    assert after == before + 2
    assert all(0 <= r["documentados"] <= r["objetos"] for r in metadados.documentation_coverage(applied))


def test_store_roundtrip_and_resilience(isolated_state, monkeypatch):
    store.save({"applied_meta": {"a.b.c": "texto"}})
    assert store.load() == {"applied_meta": {"a.b.c": "texto"}}

    isolated_state.write_text("{ isto não é json", encoding="utf-8")
    assert store.load() == {}, "arquivo corrompido não pode derrubar o portal"

    store.reset()
    assert not isolated_state.exists()

    monkeypatch.setenv("GOVERNANCA_STATE_FILE", "off")
    store.save({"x": 1})
    assert store.load() == {}
