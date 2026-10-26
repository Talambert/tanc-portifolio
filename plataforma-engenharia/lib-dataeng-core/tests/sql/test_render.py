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


@pytest.mark.parametrize(
    "sql",
    [
        "BEGIN\nDECLARE x INT64;\nSET x=1;\nSELECT x;\nEND;",
        "IF 1=1 THEN SELECT 1; END IF;",
        "BEGIN SELECT 1; END;",
        "BEGIN TRANSACTION; SELECT 1; COMMIT;",
        "SELECT 1;\nWHILE x < 3 DO SELECT 1; END WHILE;",
        "SELECT 1; END;",
        "LOOP SELECT 1; END LOOP;",
        "FOR r IN (SELECT 1) DO SELECT 1; END FOR;",
        "REPEAT SELECT 1; UNTIL true END REPEAT;",
        "CASE x WHEN 1 THEN SELECT 1; END CASE;",
    ],
)
def test_split_rejects_procedural_blocks(sql):
    with pytest.raises(SqlError) as exc:
        split_statements(sql)
    msg = str(exc.value)
    assert "blocos procedurais (BEGIN/IF/LOOP...) não são suportados" in msg
    assert "linha " in msg
    assert "SELECT 1" not in msg


def test_split_procedural_error_reports_line():
    with pytest.raises(SqlError, match="linha 3"):
        split_statements("SELECT 1;\n-- c\nIF a THEN SELECT 1; END IF;")


def test_split_does_not_reject_case_inside_expression():
    assert split_statements("SELECT CASE WHEN a THEN 1 END AS c FROM t; SELECT 2") == [
        "SELECT CASE WHEN a THEN 1 END AS c FROM t",
        "SELECT 2",
    ]


def test_split_does_not_reject_quoted_or_string_keywords():
    assert split_statements("SELECT 'BEGIN; END' AS a; SELECT `IF` FROM t") == [
        "SELECT 'BEGIN; END' AS a",
        "SELECT `IF` FROM t",
    ]


def test_split_triple_quoted_strings():
    sql = "SELECT '''a;b''' AS x; SELECT \"\"\"c;d\"\"\" AS y"
    assert split_statements(sql) == ["SELECT '''a;b''' AS x", 'SELECT """c;d""" AS y']


def test_split_raw_string_and_escaped_quote():
    sql = "SELECT r'a\\b;c' AS x; SELECT 'it\\'s;ok' AS y"
    assert split_statements(sql) == ["SELECT r'a\\b;c' AS x", "SELECT 'it\\'s;ok' AS y"]


def test_split_hash_comment():
    assert split_statements("SELECT 1; # c; d\nSELECT 2 # fim; x") == ["SELECT 1", "SELECT 2"]


def test_split_crlf():
    assert split_statements("SELECT 1; -- a;\r\nSELECT 2;\r\n") == ["SELECT 1", "SELECT 2"]


@pytest.mark.parametrize("marker", ["${ a }", "${a-b}", "${}", "${a"])
def test_render_malformed_marker_raises_with_line(marker):
    with pytest.raises(SqlError) as exc:
        render_sql(f"SELECT 1;\nSELECT {marker} FROM t", {"a": "1"}, source="q.sql")
    msg = str(exc.value)
    assert "linha 2" in msg and marker in msg and "q.sql" in msg


def test_render_undefined_param_inside_comment_also_fails():
    with pytest.raises(SqlError, match=r"\$\{x\}"):
        render_sql("SELECT 1 -- usa ${x}", {})
