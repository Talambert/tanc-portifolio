"""Checagem barata dos fluxos de CI: garante as propriedades de segurança sem depender do GitHub."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOWS_DIR = Path(__file__).resolve().parents[3] / ".github" / "workflows"
EXPECTED = ["lib-dataeng-core.yml", "template-pipeline.yml"]
EVENT_EXPRESSION = re.compile(r"\$\{\{\s*github\.event")


def violations(workflow: dict[Any, Any], text: str) -> list[str]:
    """Lista as violações das regras de segurança/higiene de um fluxo já carregado (`text` é o YAML bruto)."""
    problems: list[str] = []
    if "permissions" not in workflow:
        problems.append("falta `permissions` no topo")
    if "pull_request_target" in text:
        problems.append("usa `pull_request_target`")
    # PyYAML (YAML 1.1) lê a chave `on` como True
    triggers = workflow.get("on", workflow.get(True))
    if not isinstance(triggers, dict):
        problems.append("`on` deve ser um mapa de gatilhos")
        triggers = {}
    for event in ("push", "pull_request"):
        if not (triggers.get(event) or {}).get("paths"):
            problems.append(f"gatilho `{event}` sem `paths`")
    jobs = workflow.get("jobs") or {}
    if not jobs:
        problems.append("nenhum job")
    for name, job in jobs.items():
        if "timeout-minutes" not in job:
            problems.append(f"job `{name}` sem `timeout-minutes`")
        for step in job.get("steps", []):
            if EVENT_EXPRESSION.search(str(step.get("run", ""))):
                problems.append(f"job `{name}`: `${{{{ github.event... }}}}` interpolado em `run:`")
            if (
                str(step.get("uses", "")).startswith("actions/checkout")
                and (step.get("with") or {}).get("persist-credentials") is not False
            ):
                problems.append(f"job `{name}`: checkout sem `persist-credentials: false`")
    return problems


def _load(path: Path) -> tuple[dict[Any, Any], str]:
    text = path.read_text(encoding="utf-8")
    return yaml.safe_load(text), text


def test_workflow_files_exist() -> None:
    assert sorted(p.name for p in WORKFLOWS_DIR.glob("*.yml")) == sorted(EXPECTED)


@pytest.mark.parametrize("name", EXPECTED)
def test_workflow_has_no_violations(name: str) -> None:
    workflow, text = _load(WORKFLOWS_DIR / name)
    assert violations(workflow, text) == []


@pytest.mark.parametrize("name", EXPECTED)
def test_workflow_permissions_are_read_only_and_matrix_does_not_fail_fast(name: str) -> None:
    workflow, _ = _load(WORKFLOWS_DIR / name)
    assert workflow["permissions"] == {"contents": "read"}
    for job in workflow["jobs"].values():
        strategy = job.get("strategy")
        if strategy:
            assert strategy["fail-fast"] is False


def test_checker_rejects_an_unsafe_workflow() -> None:
    text = """
on:
  pull_request_target:
  push:
jobs:
  x:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: echo "${{ github.event.pull_request.title }}"
"""
    problems = violations(yaml.safe_load(text), text)
    assert "falta `permissions` no topo" in problems
    assert "usa `pull_request_target`" in problems
    assert "gatilho `push` sem `paths`" in problems
    assert "job `x` sem `timeout-minutes`" in problems
    assert any("interpolado em `run:`" in p for p in problems)
    assert any("persist-credentials" in p for p in problems)
