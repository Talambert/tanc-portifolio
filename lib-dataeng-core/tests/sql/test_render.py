import pytest

from dataeng_core.base.errors import SqlError
from dataeng_core.sql.render import render_sql, split_statements


def test_render_replaces_params():
    sql = "SELECT * FROM `${projeto}.${dataset}.t` WHERE d = '${dia}'"
    out = render_sql(sql, {"projeto": "p", "dataset": "silver", "dia": "2026-01-01"})
    assert out == "SELECT * FROM `p.silver.t` WHERE d = '2026-01-01'"


def test_render_missing_param_raises_before_anything_runs():
    with pytest.raises(SqlError) as exc:
        render_sql("SELECT ${a}, ${b}", {"a": "1"}, source="sql/carga.sql")
    msg = str(exc.value)
    assert "b" in msg and "${b}" in msg
    assert "sql/carga.sql" in msg
    assert "${a}" not in msg


def test_render_missing_param_without_source_still_names_param():
    with pytest.raises(SqlError, match=r"\$\{x\}"):
        render_sql("SELECT ${x}", {})


def test_render_lists_every_missing_param_once():
    with pytest.raises(SqlError) as exc:
        render_sql("${b} ${a} ${b}", {})
    msg = str(exc.value)
    assert msg.count("${b}") == 1 and "${a}" in msg


def test_render_does_not_reprocess_values():
    out = render_sql("SELECT '${a}', '${b}'", {"a": "x;'${b}", "b": "y"})
    assert out == "SELECT 'x;'${b}', 'y'"


def test_render_without_placeholders_is_identity():
    assert render_sql("SELECT '$$' AS a, '$x'", {}) == "SELECT '$$' AS a, '$x'"


def test_split_ignores_semicolon_inside_string_literal():
    sql = "INSERT INTO t VALUES ('a;b'); SELECT 'it''s; ok' AS x; SELECT `c;1` FROM t"
    assert split_statements(sql) == [
        "INSERT INTO t VALUES ('a;b')",
        "SELECT 'it''s; ok' AS x",
        "SELECT `c;1` FROM t",
    ]


def test_split_ignores_trailing_comment_and_blank_statements():
    sql = "SELECT 1;\n;\n  ;\nSELECT 2; -- fim; com ponto e virgula\n/* outro; */"
    assert split_statements(sql) == ["SELECT 1", "SELECT 2"]


def test_split_trailing_comment_without_final_semicolon():
    assert split_statements("SELECT 1; SELECT 2 -- x; y") == ["SELECT 1", "SELECT 2"]


def test_split_keeps_inner_comments_with_semicolons():
    sql = "SELECT 1 /* a;b */ AS x -- c;d\n, 2 AS y;"
    assert split_statements(sql) == ["SELECT 1 /* a;b */ AS x -- c;d\n, 2 AS y"]


def test_split_empty_or_only_comments_gives_empty_list():
    assert split_statements("") == []
    assert split_statements("  \n-- só comentário;\n/* x */") == []


def test_split_unterminated_string_raises_sql_error():
    with pytest.raises(SqlError, match="linha 2"):
        split_statements("SELECT 1;\nSELECT 'abc")


def test_split_unknown_dialect_raises_sql_error():
    with pytest.raises(SqlError, match="dialeto"):
        split_statements("SELECT 1", dialect="nao_existe")
