# 🛡️ Portal de Governança de Dados

![Status](https://img.shields.io/badge/status-prot%C3%B3tipo_em_simula%C3%A7%C3%A3o-orange)
![Dataplex](https://img.shields.io/badge/Dataplex-4285F4?logo=googlecloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?logo=googlebigquery&logoColor=white)
![Vertex AI](https://img.shields.io/badge/Vertex_AI-4285F4?logo=googlecloud&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Cloud Run](https://img.shields.io/badge/Cloud_Run-4285F4?logo=googlecloud&logoColor=white)

Portal web e biblioteca **genéricos**, para qualquer dataset do BigQuery, que usam **Dataplex** e **IA (Gemini no Vertex AI)** para acelerar três tarefas de governança: **documentar os dados (metadados)**, **proteger a qualidade (data quality)** e **responder perguntas sobre os dados com citação da fonte (catálogo)**. Tudo com **revisão humana** antes de qualquer gravação.

> Projeto de portfólio. Demonstrado com os dados de [combustíveis](../combustiveis-pipeline-gcp/) e com um dataset público do BigQuery, para provar que não depende de uma base específica.

## 🎯 O problema

Tabelas sem descrição e sem regras de qualidade custam caro: gente perde tempo para entender o dado, ou usa o dado errado. Documentar e escrever regras à mão, tabela por tabela, não escala, e a IA solta, sem contexto, inventa. O projeto dá **contexto real à IA**, **exige evidência** e **mede a qualidade do que ela propõe**.

## 🖥️ O portal

```text
 🏠 Início            visão geral, busca rápida no catálogo e estado das últimas execuções
 🛡️ Governança
     ├─ 🏷️ Metadados        lista de tabelas → sugestões → revisão → gravação
     ├─ ✅ Data Quality      lista de tabelas → profiling → regras → scans → resultados
     └─ 📚 Catálogo          pergunte sobre os dados, com citação da fonte
 ⚙️ Configurações     projeto GCP, modelo, fontes, limites de custo
```

O portal roda como um **serviço no Cloud Run**, com autenticação por Google Login (IAP) e Service Account de menor privilégio. O menu foi pensado para receber outros módulos no futuro.

## 🧩 O que será entregue

**Módulo de Metadados**
- 📥 Entrada por lista de tabelas, dataset ou arquivo de configuração
- 🧭 Contexto montado do **Dataplex** (entradas, aspectos, glossário), dos metadados existentes e de uma **amostra mascarada** do BigQuery
- ✍️ Sugestões estruturadas (descrições, classificação, termos de glossário) com **confiança e evidência**
- 🙋 Tela de revisão com a diferença entre o existente e o proposto, e **gravação auditável**

**Módulo de Data Quality**
- 📊 **Profiling no Dataplex** a partir da lista de tabelas
- 🤖 **Regras propostas pelo Gemini**, comparadas com as recomendações do próprio Dataplex
- 🧪 **Teste das regras em dado limpo** antes de publicar, para evitar falsos alarmes
- 📝 **Regras como código** (YAML versionado) e criação dos *scans* de qualidade no Dataplex
- 📈 Resultados exportados ao BigQuery e exibidos no portal

**Módulo de Catálogo (RAG)**
- 💬 Perguntas em português ("o que significa esta coluna?", "de onde vem esta tabela?") respondidas **com citação da fonte**
- 🔌 **Fontes plugáveis:** Dataplex, esquema do BigQuery, arquivos Markdown e PDF e, opcionalmente, dbt
- 🔎 Busca vetorial no BigQuery com embeddings do Vertex AI e **reindexação incremental**
- 🙅 **"Não sei" quando a fonte não sustenta** a resposta, em vez de inventar
- 🔗 Aproveita as descrições que o módulo de Metadados grava: quanto mais documentado, melhor a resposta

**Plataforma**
- 📦 Biblioteca Python reutilizável, com CLI e uso em tarefas do Airflow ou CI
- 📏 **Avaliação medida nos três módulos:** qualidade das descrições contra descrições reais, eficácia das regras com defeitos injetados e qualidade da recuperação e das respostas do catálogo
- 💸 Limites de custo, auditoria e mascaramento de dados sensíveis

## 🏗️ Arquitetura (visão geral)

```text
 Portal (Streamlit, Cloud Run) ─┐
 CLI                            ├──▶ Biblioteca  ──▶ Dataplex · BigQuery · Vertex AI (Gemini)
 Airflow / CI                  ─┘      │
                                       └─ módulos: metadados · qualidade · catálogo · núcleo (auth, config, auditoria, mascaramento)
```

## 📍 Status

🟡 **Protótipo navegável em modo simulação.** O portal já roda de ponta a ponta com **dados fictícios** (dois datasets: combustíveis e vendas), sem nenhuma chamada ao GCP. O que é real e o que é simulado:

| Parte | Hoje |
|---|---|
| Portal, menu, telas de revisão, aprovação e auditoria (com desfazer) | ✅ Real |
| Mascaramento de dados sensíveis | ✅ Real, com testes |
| Avaliação das regras de qualidade (não nulo, único, faixa, conjunto, regex), teste em dado limpo e geração do YAML | ✅ Real, sobre amostras fictícias |
| Busca no catálogo, citação de fonte e recusa quando não há fonte | ✅ Real, mas lexical e sem LLM |
| Sugestões de metadados e propostas de regras | 🧪 Simulado (dados curados) |
| Profiling e *scans* no Dataplex, leitura do BigQuery, Gemini, embeddings | ⬜ A fazer (modo real) |
| Deploy no Cloud Run com login | ⬜ A fazer |

📋 Plano completo: [planos/08-portal-governanca-dados.md](../planos/08-portal-governanca-dados.md) (Fases 22 a 29). O protótipo antecipa a Fase 25 (portal) com dados simulados; o modo real segue as Fases 22 a 24, 26 e 27.

## ▶️ Como rodar (modo simulação)

Em Git Bash, a partir desta pasta:

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements-dev.txt

streamlit run portal/app.py     # abre o portal no navegador
python -m pytest tests -q       # testes da lógica e da interface
```

**Um roteiro de 2 minutos para ver tudo funcionando:**
1. **Metadados:** *Gerar sugestões* → note que a descrição escrita por pessoa e os itens de baixa confiança vêm desmarcados → *Aprovar e gravar*. Depois teste *Desfazer*.
2. **Data Quality:** *Executar profiling* → *Propor regras* (uma regra gera falso alarme no teste em dado limpo e vem desmarcada) → *Aprovar* → *Publicar scans* e veja as violações que as regras pegam.
3. **Catálogo:** pergunte *"O que significa o valor de compra?"* (responde com a fonte) e *"Qual a cotação do dólar hoje?"* (recusa). As descrições gravadas no passo 1 já aparecem nas respostas.

## 📁 Estrutura atual

```text
portal-governanca-dados/
├── portal/
│   ├── app.py              navegação (Início · Governança · Sistema)
│   ├── ui.py               estado da sessão e elementos compartilhados
│   └── views/              inicio · metadados · qualidade · catalogo · configuracoes
├── src/governanca/
│   ├── core/               config · masking · audit
│   ├── metadados/          sugestões e política de o que pré-selecionar
│   ├── qualidade/          regras, avaliação, profiling e YAML
│   ├── catalogo/           corpus, busca e resposta com citação
│   └── mock/               dados fictícios (substituídos no modo real)
└── tests/                  lógica (mascaramento, regras, catálogo) e fluxos da interface
```

## 🛡️ Princípios de projeto

- **Genérico por padrão:** nada específico de uma base de dados.
- **Biblioteca primeiro:** a lógica não depende da tela.
- **Propor, não aplicar:** nada é gravado ou criado sem aprovação.
- **Contexto antes de geração:** a IA recebe o que já se sabe sobre o dado.
- **Fidelidade acima de fluência:** o catálogo só afirma o que está nas fontes e sempre as cita.
- **Amostra mínima e mascarada:** só o necessário vai ao modelo.
- **Medir a qualidade:** proposta sem avaliação é opinião.
