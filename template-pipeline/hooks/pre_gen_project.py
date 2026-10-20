"""Valida as respostas antes de gerar: mensagens claras para o que esta versão do template não entrega."""

import re
import sys

NOME = "{{ cookiecutter.nome_projeto }}"
TIPO = "{{ cookiecutter.tipo_projeto }}"
ORIGENS = "{{ cookiecutter.origens }}"
SUPORTADAS = ("arquivo", "api")


def erros() -> list[str]:
    problemas = []
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", NOME):
        problemas.append(
            f"nome_projeto '{NOME}' inválido: use letras, dígitos, '-' ou '_', começando por letra "
            "(vira o nome do pipeline, do Cloud Run Job e da DAG)."
        )
    if TIPO != "ingestao":
        problemas.append(f"tipo_projeto '{TIPO}' chega na Tarefa 23; por enquanto só 'ingestao' é gerado.")
    origens = [o.strip() for o in ORIGENS.split(",")]
    if not ORIGENS.strip() or "" in origens:
        problemas.append("origens vazio: informe uma ou mais entre arquivo, api, separadas por vírgula.")
    for origem in origens:
        if origem == "pubsub":
            problemas.append("a origem 'pubsub' chega na Tarefa 19; por enquanto use 'arquivo' e/ou 'api'.")
        elif origem and origem not in SUPORTADAS:
            problemas.append(f"origem '{origem}' desconhecida; valores aceitos: {', '.join(SUPORTADAS)}.")
    if len(set(origens)) != len(origens):
        problemas.append(f"origens repetidas em '{ORIGENS}'; cite cada origem uma vez.")
    return problemas


if __name__ == "__main__":
    lista = erros()
    if lista:
        print("ERRO ao gerar o projeto:\n- " + "\n- ".join(lista), file=sys.stderr)
        sys.exit(1)
