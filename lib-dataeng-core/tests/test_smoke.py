import importlib
import re

import pytest

import dataeng_core

LAYERS = [
    "base", "storage", "warehouse", "pubsub", "sources", "transform",
    "quality", "sql", "steps", "jobs", "airflow", "cli",
]


def test_version_is_semver() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", dataeng_core.__version__)


@pytest.mark.parametrize("layer", LAYERS)
def test_all_layers_importable(layer: str) -> None:
    assert importlib.import_module(f"dataeng_core.{layer}")
