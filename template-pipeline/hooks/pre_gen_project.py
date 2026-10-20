"""Valida TODAS as respostas antes de gerar qualquer arquivo.

As variáveis entram em YAML, TOML, Dockerfile, SQL e Python; por isso o formato é estrito (lista branca por
regex) e a mensagem nomeia a variável, o valor recusado e o formato esperado. Os valores chegam como literais
JSON (`tojson`), então aspas, barras e quebras de linha não quebram este próprio arquivo.
"""

import re
import sys

V = {
    "nome_projeto": {{ cookiecutter.nome_projeto | tojson }},
    "origens": {{ cookiecutter.origens | tojson }},
    "projeto_gcp": {{ cookiecutter.projeto_gcp | tojson }},
    "regiao": {{ cookiecutter.regiao | tojson }},
    "bucket_bronze": {{ cookiecutter.bucket_bronze | tojson }},
    "dataset_silver": {{ cookiecutter.dataset_silver | tojson }},
    "dataset_gold": {{ cookiecutter.dataset_gold | tojson }},
    "agenda_cron": {{ cookiecutter.agenda_cron | tojson }},
    "versao_biblioteca": {{ cookiecutter.versao_biblioteca | tojson }},
    "repositorio_biblioteca": {{ cookiecutter.repositorio_biblioteca | tojson }},
    "indice_artifact_registry": {{ cookiecutter.indice_artifact_registry | tojson }},
}

SUPORTADAS = ("arquivo", "api")
_CRON_CAMPO = r"[A-Za-z0-9*/,?-]+"
_ALIASES = "yearly|annually|monthly|weekly|daily|midnight|hourly|once"
_URL = r"https://[A-Za-z0-9._/-]+"

# variável -> (regex completa, descrição do formato esperado)
FORMATOS = {
    "nome_projeto": (r"[A-Za-z][A-Za-z0-9_-]*", "letras, dígitos, '-' ou '_', começando por letra (ex.: meu-pipeline)"),
    "projeto_gcp": (r"[a-z][a-z0-9-]{4,28}[a-z0-9]", "id de projeto GCP: 6 a 30 caracteres, minúsculas, dígitos e '-'"),
    "regiao": (r"[a-z]+-[a-z]+[0-9]+", "região GCP, ex.: southamerica-east1"),
    "bucket_bronze": (r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]", "nome de bucket: 3 a 63 caracteres, minúsculas, dígitos, '.', '_' e '-'"),
    "dataset_silver": (r"[A-Za-z_][A-Za-z0-9_]{0,1023}", "dataset do BigQuery: letras, dígitos e '_', sem '-', começando por letra ou '_'"),
    "dataset_gold": (r"[A-Za-z_][A-Za-z0-9_]{0,1023}", "dataset do BigQuery: letras, dígitos e '_', sem '-', começando por letra ou '_'"),
    "agenda_cron": (
        "(?:" + _CRON_CAMPO + "(?: " + _CRON_CAMPO + ")" + "{4}" + "|@(?:" + _ALIASES + "))",
        "5 campos cron separados por um espaço (ex.: 0 6 * * 1) ou um alias (@daily, @hourly, @weekly, @monthly, ...)",
    ),
    "versao_biblioteca": (r"\d+\.\d+\.\d+", "versão X.Y.Z (ex.: 0.1.0)"),
    "repositorio_biblioteca": (_URL, "URL https sem espaços, aspas nem metacaracteres de shell (ex.: https://github.com/org/repo.git)"),
    "indice_artifact_registry": (_URL, "URL https sem espaços, aspas nem metacaracteres de shell (ex.: https://REGIAO-python.pkg.dev/projeto/repo/simple/)"),
}

def erros() -> list[str]:
    problemas = []
    for nome, (regex, formato) in FORMATOS.items():
        if not re.fullmatch(regex, V[nome]):
            problemas.append(f"{nome} = {V[nome]!r} inválido; formato esperado: {formato}.")
    origens = [o.strip() for o in V["origens"].split(",")]
    if "" in origens:
        problemas.append(f"origens = {V['origens']!r} inválido; informe uma ou mais entre {', '.join(SUPORTADAS)}, separadas por vírgula.")
    for origem in origens:
        if origem == "pubsub":
            problemas.append("origens: a origem 'pubsub' chega na Tarefa 19; por enquanto use 'arquivo' e/ou 'api'.")
        elif origem and origem not in SUPORTADAS:
            problemas.append(f"origens: origem {origem!r} desconhecida; valores aceitos: {', '.join(SUPORTADAS)}.")
    if len(set(origens)) != len(origens):
        problemas.append(f"origens = {V['origens']!r} tem origens repetidas; cite cada origem uma vez.")
    return problemas


if __name__ == "__main__":
    lista = erros()
    if lista:
        print("ERRO ao gerar o projeto:\n- " + "\n- ".join(lista), file=sys.stderr)
        sys.exit(1)
