from pathlib import Path

import pytest
from pydantic import ValidationError

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import Environment, load_environment, load_yaml, substitute


def test_substitute_nested_structures() -> None:
    value = {"a": "x-${p}", "b": ["${p}", {"c": "${q}/${p}"}]}
    out = substitute(value, {"p": "1", "q": "2"})
    assert out == {"a": "x-1", "b": ["1", {"c": "2/1"}]}


def test_substitute_strict_names_the_missing_param() -> None:
    with pytest.raises(ConfigError) as exc:
        substitute("select * from ${dataset_gold}.t", {"p": "1"})
    assert "dataset_gold" in str(exc.value)


def test_substitute_non_strict_keeps_placeholder() -> None:
    assert substitute("${a}-${b}", {"a": "1"}, strict=False) == "1-${b}"


def test_non_string_values_untouched() -> None:
    value = {"n": 3, "f": 1.5, "b": True, "z": None, "l": [1, "${p}"]}
    assert substitute(value, {"p": "x"}) == {"n": 3, "f": 1.5, "b": True, "z": None, "l": [1, "x"]}


def test_load_yaml_reports_file_on_syntax_error(tmp_path: Path) -> None:
    f = tmp_path / "ruim.yaml"
    f.write_text("a: 1\nb: [1, 2\nc: 3\n", encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_yaml(f)
    assert "ruim.yaml" in str(exc.value)
    assert "linha" in str(exc.value)


def test_load_yaml_returns_dict(tmp_path: Path) -> None:
    f = tmp_path / "ok.yaml"
    f.write_text("a: 1\n", encoding="utf-8")
    assert load_yaml(f) == {"a": 1}


def test_environment_requires_fields_and_exposes_params(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        Environment.model_validate({"modo": "local"})
    f = tmp_path / "env.yaml"
    f.write_text(
        "modo: gcp\nprojeto_gcp: p\nregiao: r\nbucket_bronze: b\ndataset_silver: s\ndataset_gold: g\n",
        encoding="utf-8",
    )
    env = load_environment(f)
    assert env.service_account is None
    assert env.pasta_local == ".local"
    params = env.params()
    assert params["projeto_gcp"] == "p"
    assert params["dataset_gold"] == "g"
    assert params["bucket_bronze"] == "b"
    assert all(isinstance(v, str) for v in params.values())


def test_load_environment_invalid_raises_config_error(tmp_path: Path) -> None:
    f = tmp_path / "env.yaml"
    f.write_text("modo: local\n", encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_environment(f)
    assert "env.yaml" in str(exc.value)


def test_load_yaml_non_utf8_is_actionable(tmp_path: Path) -> None:
    f = tmp_path / "latin.yaml"
    f.write_bytes("nome: açúcar\n".encode("latin-1"))
    with pytest.raises(ConfigError) as exc:
        load_yaml(f)
    assert "UTF-8" in exc.value.detail and "latin.yaml" in exc.value.detail
    assert "UTF-8" in exc.value.fix


def test_environment_rejects_unknown_field_naming_it_and_the_valid_ones(tmp_path: Path) -> None:
    f = tmp_path / "env.yaml"
    f.write_text(
        "modo: local\nprojeto_gcp: p\nregiao: r\nbucket_bronze: b\ndataset_silver: s\ndataset_gold: g\n"
        "service_acount: x\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError) as exc:
        load_environment(f)
    assert "service_acount" in exc.value.detail and "service_account" in exc.value.detail
