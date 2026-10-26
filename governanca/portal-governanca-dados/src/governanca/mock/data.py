"""Dados fictícios para o modo simulação.

Dois datasets de propósito, para mostrar que o portal não depende de uma base específica:
`combustiveis_gold` (modelo dimensional dos preços da ANP) e `vendas_demo` (pedidos e clientes).
Quando o modo real existir, estas funções são substituídas por leituras do Dataplex e do BigQuery.
"""
import random
from functools import lru_cache

import pandas as pd

UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
    "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]
PRODUTOS = ["GASOLINA", "GASOLINA ADITIVADA", "ETANOL", "DIESEL", "DIESEL S10", "GNV"]
BANDEIRAS = ["IPIRANGA", "BRANCA", "RAIZEN", "ALE", "OUTRAS"]

# Estrutura: dataset -> tabela -> {"description": texto humano existente, "columns": [(nome, tipo, descrição existente)]}
DATASETS: dict[str, dict[str, dict]] = {
    "combustiveis_gold": {
        "fct_preco_combustivel": {
            "description": "Preços de combustíveis.",
            "columns": [
                ("sk_posto", "STRING", ""),
                ("sk_produto", "STRING", ""),
                ("sk_localidade", "STRING", ""),
                ("sk_data", "INT64", ""),
                ("data_coleta", "DATE", ""),
                ("valor_venda", "NUMERIC", "Preço de venda ao consumidor"),
                ("valor_compra", "NUMERIC", ""),
            ],
        },
        "dim_posto": {
            "description": "",
            "columns": [
                ("sk_posto", "STRING", ""),
                ("cnpj_revenda", "STRING", ""),
                ("revenda", "STRING", ""),
                ("bandeira", "STRING", "Bandeira"),
                ("endereco", "STRING", ""),
            ],
        },
        "dim_produto": {
            "description": "",
            "columns": [
                ("sk_produto", "STRING", ""),
                ("produto", "STRING", ""),
                ("familia", "STRING", ""),
                ("unidade_medida", "STRING", ""),
            ],
        },
        "dim_localidade": {
            "description": "",
            "columns": [
                ("sk_localidade", "STRING", ""),
                ("municipio", "STRING", ""),
                ("uf", "STRING", ""),
                ("regiao", "STRING", ""),
            ],
        },
    },
    "vendas_demo": {
        "pedidos": {
            "description": "",
            "columns": [
                ("id_pedido", "INT64", ""),
                ("id_cliente", "INT64", ""),
                ("email_cliente", "STRING", ""),
                ("data_pedido", "DATE", ""),
                ("valor_total", "NUMERIC", ""),
                ("status", "STRING", ""),
            ],
        },
        "clientes": {
            "description": "Cadastro de clientes.",
            "columns": [
                ("id_cliente", "INT64", ""),
                ("nome", "STRING", ""),
                ("email", "STRING", ""),
                ("cpf", "STRING", "CPF do cliente"),
                ("uf", "STRING", ""),
                ("criado_em", "TIMESTAMP", ""),
            ],
        },
    },
}

# (descrição, confiança, evidência, classificação, candidata a dado sensível)
ColumnSuggestion = tuple[str, str, str, str, bool]

# Sugestões "do modelo". Uma delas (status) é de propósito de baixa confiança, para mostrar a honestidade.
TABLE_SUGGESTIONS: dict[tuple[str, str], tuple[str, str, str, str]] = {
    ("combustiveis_gold", "fct_preco_combustivel"): (
        "Preços de combustíveis coletados por posto, produto e data de coleta (um registro por combinação).",
        "alta", "Colunas sk_posto, sk_produto, sk_data e valor_venda; padrão de tabela fato.", "Fato / Preços",
    ),
    ("combustiveis_gold", "dim_posto"): (
        "Cadastro dos postos revendedores: CNPJ, razão social, bandeira e endereço.",
        "alta", "Colunas cnpj_revenda, revenda, bandeira e endereco.", "Dimensão / Postos",
    ),
    ("combustiveis_gold", "dim_produto"): (
        "Catálogo de produtos pesquisados, com a família e a unidade de medida.",
        "alta", "Colunas produto, familia e unidade_medida; valores como GASOLINA, ETANOL e GNV na amostra.", "Dimensão / Produtos",
    ),
    ("combustiveis_gold", "dim_localidade"): (
        "Localidades pesquisadas: município, UF e região.",
        "alta", "Colunas municipio, uf e regiao; UFs válidas na amostra.", "Dimensão / Geografia",
    ),
    ("vendas_demo", "pedidos"): (
        "Pedidos realizados pelos clientes, com valor total e situação.",
        "média", "Colunas id_pedido, id_cliente, valor_total e status.", "Fato / Vendas",
    ),
    ("vendas_demo", "clientes"): (
        "Cadastro de clientes com contato, documento e UF.",
        "alta", "Colunas nome, email, cpf e uf.", "Dimensão / Clientes",
    ),
}

COLUMN_SUGGESTIONS: dict[tuple[str, str], dict[str, ColumnSuggestion]] = {
    ("combustiveis_gold", "fct_preco_combustivel"): {
        "sk_posto": ("Chave substituta do posto (liga à dim_posto).", "alta", "Prefixo sk_ e mesma coluna em dim_posto.", "Chave", False),
        "sk_produto": ("Chave substituta do produto (liga à dim_produto).", "alta", "Prefixo sk_ e mesma coluna em dim_produto.", "Chave", False),
        "sk_localidade": ("Chave substituta da localidade (liga à dim_localidade).", "alta", "Prefixo sk_ e mesma coluna em dim_localidade.", "Chave", False),
        "sk_data": ("Chave da data de coleta no formato AAAAMMDD.", "média", "Inteiros de 8 dígitos na amostra, coerentes com data_coleta.", "Chave", False),
        "data_coleta": ("Data em que o preço foi coletado no posto.", "alta", "Tipo DATE e nome da coluna.", "Data", False),
        "valor_venda": ("Preço de venda ao consumidor final, em reais por unidade de medida do produto.", "alta", "Descrição existente, valores entre 4 e 9 na amostra.", "Medida", False),
        "valor_compra": ("Preço de compra do combustível pelo posto; opcional e, na amostra, quase sempre vazio.", "média", "Mais de 85% de nulos na amostra.", "Medida", False),
    },
    ("combustiveis_gold", "dim_posto"): {
        "sk_posto": ("Chave substituta do posto.", "alta", "Prefixo sk_ e valores únicos.", "Chave", False),
        "cnpj_revenda": ("CNPJ do posto revendedor, no formato 00.000.000/0000-00.", "alta", "Padrão de CNPJ na amostra.", "Identificador de empresa", True),
        "revenda": ("Razão social do posto revendedor.", "alta", "Textos em maiúsculas terminando em LTDA ou S/A.", "Texto", False),
        "bandeira": ("Bandeira (marca) do posto, como IPIRANGA ou BRANCA (sem bandeira).", "alta", "Valores da amostra e descrição existente.", "Categoria", False),
        "endereco": ("Endereço do posto: rua, número, complemento e bairro.", "média", "Textos com logradouro e número.", "Texto", False),
    },
    ("combustiveis_gold", "dim_produto"): {
        "sk_produto": ("Chave substituta do produto.", "alta", "Prefixo sk_ e valores únicos.", "Chave", False),
        "produto": ("Nome do combustível, como GASOLINA, ETANOL, DIESEL S10 ou GNV.", "alta", "Valores da amostra.", "Categoria", False),
        "familia": ("Agrupamento do produto: gasolina, etanol, diesel ou GNV.", "média", "Valores derivados do nome do produto.", "Categoria", False),
        "unidade_medida": ("Unidade do preço: R$/litro para líquidos e R$/m³ para GNV.", "alta", "Valores 'R$ / litro' e 'R$ / m³' na amostra.", "Categoria", False),
    },
    ("combustiveis_gold", "dim_localidade"): {
        "sk_localidade": ("Chave substituta da localidade.", "alta", "Prefixo sk_ e valores únicos.", "Chave", False),
        "municipio": ("Nome do município onde o posto está localizado.", "alta", "Nomes de cidades em maiúsculas.", "Geografia", False),
        "uf": ("Sigla da unidade da federação (27 valores possíveis).", "alta", "Siglas de duas letras na amostra.", "Geografia", False),
        "regiao": ("Região geográfica do Brasil (N, NE, CO, SE, S).", "média", "Siglas de uma ou duas letras na amostra.", "Geografia", False),
    },
    ("vendas_demo", "pedidos"): {
        "id_pedido": ("Identificador único do pedido.", "alta", "Inteiros sequenciais e únicos.", "Chave", False),
        "id_cliente": ("Identificador do cliente que fez o pedido (liga à tabela clientes).", "alta", "Mesmo nome e tipo em clientes.", "Chave", False),
        "email_cliente": ("E-mail do cliente no momento do pedido.", "alta", "Padrão de e-mail na amostra.", "Contato pessoal", True),
        "data_pedido": ("Data em que o pedido foi realizado.", "alta", "Tipo DATE e nome da coluna.", "Data", False),
        "valor_total": ("Valor total do pedido, em reais.", "média", "Valores numéricos positivos na amostra; moeda não está explícita.", "Medida", False),
        "status": ("", "baixa", "Sem evidência suficiente: a amostra tem poucos valores e nenhum metadado explica os códigos.", "Categoria", False),
    },
    ("vendas_demo", "clientes"): {
        "id_cliente": ("Identificador único do cliente.", "alta", "Inteiros sequenciais e únicos.", "Chave", False),
        "nome": ("Nome completo do cliente.", "alta", "Textos com nome e sobrenome.", "Dado pessoal", True),
        "email": ("E-mail de contato do cliente.", "alta", "Padrão de e-mail na amostra.", "Contato pessoal", True),
        "cpf": ("CPF do cliente, no formato 000.000.000-00.", "alta", "Padrão de CPF na amostra e descrição existente.", "Documento pessoal", True),
        "uf": ("Sigla da unidade da federação do cliente.", "alta", "Siglas de duas letras na amostra.", "Geografia", False),
        "criado_em": ("Data e hora em que o cadastro do cliente foi criado.", "alta", "Tipo TIMESTAMP e nome da coluna.", "Data", False),
    },
}

# Regras propostas: (coluna, tipo, parâmetros, limiar, origem, confiança, justificativa)
RuleProposal = tuple[str, str, dict, float, str, str, str]

RULE_PROPOSALS: dict[tuple[str, str], list[RuleProposal]] = {
    ("combustiveis_gold", "fct_preco_combustivel"): [
        ("sk_posto", "nao_nulo", {}, 1.0, "dataplex_recomendada", "alta", "Chave de ligação com a dimensão; nunca pode ser vazia."),
        ("valor_venda", "nao_nulo", {}, 1.0, "dataplex_recomendada", "alta", "Medida principal da tabela."),
        ("valor_venda", "faixa", {"minimo": 0.5, "maximo": 30}, 0.99, "gemini", "alta",
         "Preço por unidade; valores fora da faixa indicam erro de unidade ou de vírgula decimal."),
        ("valor_compra", "nao_nulo", {}, 0.99, "gemini", "média",
         "Preço de compra deveria acompanhar o de venda. (Atenção: a coluna é quase sempre vazia, então esta regra tende a falhar.)"),
    ],
    ("combustiveis_gold", "dim_posto"): [
        ("sk_posto", "unico", {}, 1.0, "dataplex_recomendada", "alta", "Chave primária da dimensão."),
        ("cnpj_revenda", "regex", {"padrao": r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}"}, 0.99, "gemini", "alta",
         "CNPJ no formato padrão; pega espaços e pontuação fora do lugar."),
    ],
    ("combustiveis_gold", "dim_produto"): [
        ("produto", "unico", {}, 1.0, "gemini", "alta", "Cada produto deve aparecer uma única vez na dimensão."),
    ],
    ("combustiveis_gold", "dim_localidade"): [
        ("uf", "conjunto", {"valores": UFS}, 1.0, "gemini", "alta", "Somente as 27 siglas de UF existentes."),
    ],
    ("vendas_demo", "pedidos"): [
        ("id_pedido", "unico", {}, 1.0, "dataplex_recomendada", "alta", "Identificador do pedido."),
        ("valor_total", "faixa", {"minimo": 0, "maximo": 1_000_000}, 0.99, "gemini", "alta", "Pedido não pode ter valor negativo."),
        ("email_cliente", "regex", {"padrao": r"[\w.+-]+@[\w-]+\.[\w.-]+"}, 0.99, "gemini", "média", "E-mail em formato válido."),
    ],
    ("vendas_demo", "clientes"): [
        ("cpf", "regex", {"padrao": r"\d{3}\.\d{3}\.\d{3}-\d{2}"}, 0.99, "gemini", "alta", "CPF no formato padrão."),
        ("email", "regex", {"padrao": r"[\w.+-]+@[\w-]+\.[\w.-]+"}, 0.99, "gemini", "média", "E-mail em formato válido."),
        ("uf", "conjunto", {"valores": UFS}, 1.0, "gemini", "alta", "Somente as 27 siglas de UF existentes."),
    ],
}

# Documentos do catálogo: (fonte, tipo, referência, texto)
DOCUMENTS: list[tuple[str, str, str, str]] = [
    ("Metadados da ANP (PDF)", "arquivo", "p. 2",
     "Valor de Compra: preço de compra do combustível pelo posto revendedor. O campo é opcional e, na prática, vem vazio na maior parte dos registros."),
    ("Metadados da ANP (PDF)", "arquivo", "p. 2",
     "Valor de Venda: preço de venda ao consumidor final praticado pelo posto na data da coleta, em reais."),
    ("Metadados da ANP (PDF)", "arquivo", "p. 3",
     "Unidade de Medida: R$ / litro para combustíveis líquidos e R$ / m³ para o GNV."),
    ("Documentação do dbt", "dbt", "fct_preco_combustivel",
     "A tabela fct_preco_combustivel é construída a partir da camada silver de preços. O grão é um registro por posto, produto e data de coleta."),
    ("Documentação do dbt", "dbt", "dim_posto",
     "A tabela dim_posto vem da silver e guarda o CNPJ, a razão social, a bandeira e o endereço dos postos. A chave substituta é sk_posto."),
    ("Dicionário de dados", "arquivo", "métricas",
     "Paridade etanol/gasolina: razão entre o preço do etanol e o da gasolina. Abaixo de 70% costuma compensar abastecer com etanol."),
    ("Regras de qualidade", "arquivo", "CNPJ",
     "O CNPJ da revenda tem 14 dígitos e deve estar no formato 00.000.000/0000-00. Os arquivos da ANP trazem um espaço no início, removido na camada silver."),
    ("Regras de qualidade", "arquivo", "linhas malformadas",
     "Algumas linhas dos arquivos da ANP têm ponto e vírgula dentro do endereço, o que desloca as colunas. Essas linhas vão para a tabela de quarentena."),
    ("Dicionário de dados", "arquivo", "vendas_demo.pedidos",
     "A tabela pedidos guarda um registro por pedido. O status do pedido assume os valores CRIADO, PAGO e CANCELADO."),
    ("Dicionário de dados", "arquivo", "vendas_demo.clientes",
     "A tabela clientes guarda o cadastro dos clientes. CPF e e-mail são dados pessoais e devem ser tratados como sensíveis."),
]


def list_datasets() -> list[str]:
    return list(DATASETS)


def list_tables(dataset: str) -> list[str]:
    return list(DATASETS[dataset])


def _cnpj(rng: random.Random) -> str:
    d = [rng.randint(0, 9) for _ in range(14)]
    return f"{d[0]}{d[1]}.{d[2]}{d[3]}{d[4]}.{d[5]}{d[6]}{d[7]}/{d[8]}{d[9]}{d[10]}{d[11]}-{d[12]}{d[13]}"


def _cpf(rng: random.Random) -> str:
    d = [rng.randint(0, 9) for _ in range(11)]
    return f"{d[0]}{d[1]}{d[2]}.{d[3]}{d[4]}{d[5]}.{d[6]}{d[7]}{d[8]}-{d[9]}{d[10]}"


def get_sample(dataset: str, table: str, dirty: bool = False, n: int = 200) -> pd.DataFrame:
    """Amostra fictícia da tabela. `dirty=True` injeta defeitos conhecidos (a "realidade" que o profiling revela).

    A geração é determinística, então o resultado é guardado em cache e cada chamada recebe uma cópia
    (quem chama pode alterar o DataFrame à vontade).
    """
    return _sample(dataset, table, dirty, n).copy()


@lru_cache(maxsize=64)
def _sample(dataset: str, table: str, dirty: bool, n: int) -> pd.DataFrame:
    rng = random.Random(f"{dataset}.{table}")
    defect = lambda: dirty and rng.random() < 0.04  # noqa: E731 - 4% de defeitos

    if table == "fct_preco_combustivel":
        rows = []
        for i in range(n):
            price = round(rng.uniform(4.5, 8.5), 2)
            if defect():
                price = -price if rng.random() < 0.5 else round(price * 100, 2)
            rows.append({
                "sk_posto": f"P{rng.randint(1, 60):04d}", "sk_produto": f"PR{rng.randint(1, 6)}",
                "sk_localidade": f"L{rng.randint(1, 30):03d}", "sk_data": 20260901 + rng.randint(0, 28),
                "data_coleta": pd.Timestamp("2026-09-01") + pd.Timedelta(days=rng.randint(0, 28)),
                "valor_venda": price,
                "valor_compra": round(price * 0.85, 2) if rng.random() < 0.1 else None,
            })
        df = pd.DataFrame(rows)
        if dirty:
            df.loc[rng.sample(range(n), 3), "sk_posto"] = None
        return df

    if table == "dim_posto":
        rows = []
        for i in range(n):
            cnpj = _cnpj(rng)
            if defect():
                cnpj = " " + cnpj
            elif dirty and rng.random() < 0.02:
                cnpj = None
            rows.append({
                "sk_posto": f"P{i + 1:04d}", "cnpj_revenda": cnpj, "revenda": f"POSTO {i + 1} COMERCIO DE COMBUSTIVEIS LTDA",
                "bandeira": rng.choice(BANDEIRAS), "endereco": f"RUA {rng.randint(1, 99)}, {rng.randint(1, 999)}, CENTRO",
            })
        return pd.DataFrame(rows)

    if table == "dim_produto":
        names = PRODUTOS + (["GASOLINA", "ETANOL"] if dirty else [])
        return pd.DataFrame([
            {"sk_produto": f"PR{i + 1}", "produto": name,
             "familia": name.split()[0].title(), "unidade_medida": "R$ / m³" if name == "GNV" else "R$ / litro"}
            for i, name in enumerate(names)
        ])

    if table == "dim_localidade":
        rows = []
        for i in range(n):
            uf = rng.choice(UFS)
            if defect():
                uf = "XX"
            rows.append({"sk_localidade": f"L{i + 1:03d}", "municipio": f"MUNICIPIO {i + 1}", "uf": uf, "regiao": rng.choice(["N", "NE", "CO", "SE", "S"])})
        return pd.DataFrame(rows)

    if table == "pedidos":
        rows = []
        for i in range(n):
            total = round(rng.uniform(20, 2000), 2)
            email = f"cliente{rng.randint(1, 80)}@exemplo.com"
            if defect():
                total = -total
            if defect():
                email = f"cliente{rng.randint(1, 80)}exemplo.com"
            rows.append({
                "id_pedido": i + 1, "id_cliente": rng.randint(1, 80), "email_cliente": email,
                "data_pedido": pd.Timestamp("2026-09-01") + pd.Timedelta(days=rng.randint(0, 28)),
                "valor_total": total, "status": rng.choice(["CRIADO", "PAGO", "CANCELADO"]),
            })
        df = pd.DataFrame(rows)
        if dirty:
            df.loc[[5, 6], "id_pedido"] = 1  # chave duplicada
        return df

    if table == "clientes":
        rows = []
        for i in range(80):
            cpf = _cpf(rng)
            if defect():
                cpf = cpf.replace(".", "").replace("-", "")
            uf = rng.choice(UFS)
            if defect():
                uf = "ZZ"
            rows.append({
                "id_cliente": i + 1, "nome": f"Cliente Teste {i + 1}", "email": f"cliente{i + 1}@exemplo.com",
                "cpf": cpf, "uf": uf, "criado_em": pd.Timestamp("2025-01-01") + pd.Timedelta(days=rng.randint(0, 600)),
            })
        return pd.DataFrame(rows)

    raise KeyError(f"Tabela desconhecida na simulação: {table}")
