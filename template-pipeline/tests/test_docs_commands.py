"""Os comandos marcados em `docs/guia-novo-pipeline.md` precisam funcionar num projeto gerado de verdade.

Só os blocos ```bash precedidos da linha `<!-- testar -->` são executados. Blocos ilustrativos (pip, gcloud,
docker...) não levam a marca e nunca rodam. Cada comando precisa ser `dataeng ...`; ele é executado como
`python -m dataeng_core.cli ...` com o mesmo Python do teste, sem shell, e deve terminar com código 0.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

GUIDE = Path(__file__).resolve().parent.parent / "docs" / "guia-novo-pipeline.md"
MARKED_BLOCK = re.compile(r"<!-- testar -->[ \t]*\r?\n```bash[ \t]*\r?\n(.*?)\r?\n```", re.DOTALL)
TIMEOUT_S = 180


def extract_marked_commands(markdown: str) -> list[list[str]]:
    """Devolve, na ordem do guia, cada comando dos blocos marcados já separado em argumentos (`shlex`)."""
    commands: list[list[str]] = []
    for block in MARKED_BLOCK.findall(markdown):
        logical = re.sub(r"\\\r?\n", " ", block)  # junta linhas terminadas em `\`
        for line in logical.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                commands.append(shlex.split(line))
    return commands


def run_marked_commands(commands: list[list[str]], cwd: Path) -> list[str]:
    """Executa os comandos em `cwd` e devolve uma descrição de cada falha (lista vazia = tudo certo)."""
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
    failures: list[str] = []
    for argv in commands:
        shown = shlex.join(argv)
        if not argv or argv[0] != "dataeng":
            failures.append(f"{shown}: só comandos `dataeng ...` podem ser marcados com <!-- testar -->")
            continue
        try:
            done = subprocess.run(
                [sys.executable, "-m", "dataeng_core.cli", *argv[1:]],
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=TIMEOUT_S,
                check=False,
            )
        except subprocess.TimeoutExpired:
            failures.append(f"{shown}: passou de {TIMEOUT_S} s")
            continue
        if done.returncode != 0:
            failures.append(f"{shown}: código {done.returncode}\n{done.stdout[-800:]}\n{done.stderr[-800:]}")
    return failures


def test_extract_takes_only_marked_blocks_and_joins_continuations() -> None:
    texto = (
        "```bash\npip install x\n```\n\n"
        "<!-- testar -->\n```bash\n# comentário\ndataeng plan \\\n  --config config/pipeline.yaml\n```\n\n"
        "<!-- testar -->\n```bash\ndataeng validate-config --config config/pipeline.yaml\n```\n"
    )
    assert extract_marked_commands(texto) == [
        ["dataeng", "plan", "--config", "config/pipeline.yaml"],
        ["dataeng", "validate-config", "--config", "config/pipeline.yaml"],
    ]


def test_runner_reports_broken_and_non_dataeng_commands(tmp_path: Path) -> None:
    falhas = run_marked_commands(
        [["dataeng", "validate-config", "--config", "nao-existe.yaml"], ["rm", "-rf", "x"]], tmp_path
    )
    assert len(falhas) == 2
    assert "código 1" in falhas[0]
    assert "só comandos `dataeng" in falhas[1]


def test_guide_commands_run_on_a_generated_project(bake: Callable[..., Path]) -> None:
    assert GUIDE.is_file(), f"guia ausente: {GUIDE}"
    commands = extract_marked_commands(GUIDE.read_text(encoding="utf-8"))
    assert commands, "o guia não tem nenhum bloco marcado com <!-- testar -->"
    project = bake(tipo_projeto="hibrido", origens="arquivo", usa_python="sim")
    failures = run_marked_commands(commands, project)
    assert not failures, "comandos do guia falharam:\n" + "\n---\n".join(failures)


@pytest.mark.parametrize("trecho", ["tipo_projeto=hibrido", "origens=arquivo", "usa_python=sim"])
def test_guide_generation_command_matches_the_tested_project(trecho: str) -> None:
    assert trecho in GUIDE.read_text(encoding="utf-8")
