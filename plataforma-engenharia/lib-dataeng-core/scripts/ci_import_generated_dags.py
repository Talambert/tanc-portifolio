"""Gera projetos com o template e importa de verdade as DAGs geradas (precisa do Airflow; roda no CI, em Linux).

Uso: python plataforma-engenharia/lib-dataeng-core/scripts/ci_import_generated_dags.py <pasta-de-saida>
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from cookiecutter.main import cookiecutter

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "template-pipeline"
CASES = [
    {"tipo_projeto": "ingestao", "motor_ingestao": "ambos"},
    {"tipo_projeto": "relatorio"},
    {"tipo_projeto": "hibrido", "motor_ingestao": "cloud_run"},
]


def main(out_dir: Path) -> int:
    import airflow  # noqa: F401  (falha cedo e com erro claro se o Airflow não estiver instalado)

    imported = 0
    for index, case in enumerate(CASES):
        out = out_dir / f"caso{index}"
        out.mkdir(parents=True, exist_ok=True)
        cookiecutter(str(TEMPLATE_DIR), no_input=True, output_dir=str(out), extra_context=dict(case))
        project = next(p for p in out.iterdir() if p.is_dir())
        for dag_file in sorted((project / "dags").glob("*.py")):
            spec = importlib.util.spec_from_file_location(f"dag_caso{index}_{dag_file.stem}", dag_file)
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            dag = getattr(module, "dag", None)
            if dag is None or not hasattr(dag, "task_ids") or not dag.task_ids:
                raise SystemExit(f"{dag_file}: a DAG gerada está ausente ou vazia")
            print(f"OK {case} {dag_file.name}: dag_id={dag.dag_id} tarefas={len(dag.task_ids)}")
            imported += 1
    if imported == 0:
        raise SystemExit("nenhuma DAG importada")
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
