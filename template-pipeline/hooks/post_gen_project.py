"""Remove o que não se aplica às escolhas feitas (roda dentro da pasta do projeto gerado)."""

import shutil
from pathlib import Path

ORIGENS = [o.strip() for o in "{{ cookiecutter.origens }}".split(",")]
MOTOR = "{{ cookiecutter.motor_ingestao }}"
ROOT = Path.cwd()


def remove(*caminhos: str) -> None:
    for caminho in caminhos:
        alvo = ROOT / caminho
        if alvo.is_dir():
            shutil.rmtree(alvo)
        elif alvo.exists():
            alvo.unlink()


def main() -> None:
    # Ingestão não gera views nem scripts Spark (isso é dos tipos relatorio/hibrido).
    remove("sql/views", "spark")

    # Bloco de origem não escolhido.
    if "arquivo" not in ORIGENS:
        remove("data", "schemas/origem.yaml", "sql/ddl/silver_registros_arquivo.sql")
    if "api" not in ORIGENS:
        remove("sql/ddl/silver_registros_api.sql")
    # O teste de execução local lê a amostra; com a origem `api` ele faria chamadas de rede.
    if ORIGENS != ["arquivo"]:
        remove("tests/test_local_run.py")

    # DAG do motor não escolhido. Com um só motor, a DAG se chama dags/pipeline.py.
    if MOTOR == "cloud_run":
        remove("dags/pipeline_dataproc.py")
        (ROOT / "dags/pipeline_cloud_run.py").rename(ROOT / "dags/pipeline.py")
    elif MOTOR == "dataproc":
        remove("dags/pipeline_cloud_run.py")
        (ROOT / "dags/pipeline_dataproc.py").rename(ROOT / "dags/pipeline.py")
    else:  # ambos: o mesmo pipeline com o motor da ingestão trocado
        texto = (ROOT / "config/pipeline.yaml").read_text(encoding="utf-8")
        (ROOT / "config/pipeline_dataproc.yaml").write_text(
            texto.replace("motor: cloud_run", "motor: dataproc"), encoding="utf-8"
        )

    # Dependências: `requirements.txt` só existe com o Artifact Registry (precisa do --extra-index-url).
    if "{{ cookiecutter.origem_da_biblioteca }}" != "artifact_registry":
        remove("requirements.txt")


main()
