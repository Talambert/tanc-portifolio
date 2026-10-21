import pytest

from dataeng_core.base.errors import SqlError
from dataeng_core.sql.validate import validate_sql


def test_validate_rejects_bad_syntax_with_line():
    sql = "SELECT 1;\nSELECT 2;\nSELECT 3;\nSELECT * FROM (SELECT 1"
    with pytest.raises(SqlError) as exc:
        validate_sql(sql)
    assert "linha 4" in str(exc.value)
    assert "sintaxe inválida" in str(exc.value)


def test_validate_line_is_relative_to_whole_script():
    with pytest.raises(SqlError, match="linha 3"):
        validate_sql("SELECT 1;\n\nSELEC 2 FROM FROM t")


def test_validate_names_source_when_given():
    with pytest.raises(SqlError, match="sql/x.sql"):
        validate_sql("SELECT FROM (", source="sql/x.sql")


def test_validate_accepts_bigquery_merge_and_create_or_replace_view():
    sql = """
    CREATE OR REPLACE VIEW `p.gold.v` AS SELECT a, COUNT(*) AS n FROM `p.silver.t` GROUP BY a;
    MERGE `p.silver.t` AS d USING `p.stg.t` AS s ON d.id = s.id
    WHEN MATCHED THEN UPDATE SET d.v = s.v
    WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
    """
    assert validate_sql(sql) == []


def test_validate_warns_on_plain_insert():
    warnings = validate_sql("INSERT INTO t (a) SELECT a FROM s;")
    assert len(warnings) == 1
    assert "INSERT" in warnings[0] and "linha 1" in warnings[0]
    assert "MERGE" in warnings[0] and "WHERE NOT EXISTS" in warnings[0]


def test_validate_warning_line_points_to_statement():
    warnings = validate_sql("SELECT 1;\n\nINSERT INTO t (a) VALUES (1)")
    assert len(warnings) == 1 and "linha 3" in warnings[0]


def test_validate_does_not_warn_on_merge():
    sql = "MERGE t d USING s ON d.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"
    assert validate_sql(sql) == []


def test_validate_does_not_warn_on_insert_where_not_exists():
    sql = "INSERT INTO t (a) SELECT a FROM s WHERE NOT EXISTS (SELECT 1 FROM t WHERE t.a = s.a)"
    assert validate_sql(sql) == []


def test_validate_does_not_warn_on_insert_overwrite():
    assert validate_sql("INSERT OVERWRITE TABLE t SELECT a FROM s", dialect="spark") == []


def test_validate_semicolon_in_string_is_not_a_split():
    assert validate_sql("SELECT 'a;b' AS x; -- fim;") == []


def test_validate_empty_script_returns_no_warnings():
    assert validate_sql("-- nada\n") == []


def test_validate_error_does_not_leak_literal_values():
    with pytest.raises(SqlError) as exc:
        validate_sql("SELECT 'segredo-123' FROM (")
    assert "segredo-123" not in str(exc.value)


def test_validate_not_exists_in_select_list_does_not_count():
    sql = "INSERT INTO t (a) SELECT NOT EXISTS (SELECT 1 FROM s) AS a"
    assert len(validate_sql(sql)) == 1


def test_validate_rejects_procedural_block():
    with pytest.raises(SqlError, match="blocos procedurais"):
        validate_sql("BEGIN SELECT 1; END;")
