import re

import pytest

from dataeng_core.base import errors

SUBTYPES = [
    errors.ConfigError,
    errors.SourceError,
    errors.LoadError,
    errors.QualityError,
    errors.SqlError,
    errors.PubSubError,
]


@pytest.mark.parametrize("cls", SUBTYPES)
def test_each_error_has_actionable_text(cls: type[errors.DataEngError]) -> None:
    err = cls("detalhe", operation="op")
    assert issubclass(cls, errors.DataEngError)
    assert cls.title and cls.what and cls.fix
    assert re.fullmatch(r"[0-9a-f]{8}", err.reference)


def test_fix_per_instance_overrides_the_class_default() -> None:
    assert errors.ConfigError("x", fix="faça isto").fix == "faça isto"
    assert errors.ConfigError("x").fix == errors.ConfigError.fix
    assert errors.ConfigError.fix != "faça isto"  # a classe não é alterada
