<div align="center">

# 👋 Olá, eu sou o Tancredo

### Engenheiro de Dados · BI · Cloud

*Do dado bruto à decisão: eu construo o caminho entre os dois.*

![Data Engineering](https://img.shields.io/badge/Data_Engineering-0a5c20?style=for-the-badge)
![Business Intelligence](https://img.shields.io/badge/Business_Intelligence-1F6AA5?style=for-the-badge)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

[💼 LinkedIn](https://br.linkedin.com/in/tancredo-alambert-14a45119) · [✉️ E-mail](mailto:t.alambert@gmail.com) · [🐙 GitHub](https://github.com/Talambert)

</div>

---

## 🧭 Sobre mim

Trabalho com **dados e BI há muitos anos**, e foi nesse tempo que aprendi uma coisa: dashboard bonito não salva dado ruim. Meu trabalho é cuidar de tudo o que vem *antes* da análise, para que quem decide possa confiar no número que está vendo.

- 🏗️ **Construo pipelines e plataformas de dados** que rodam todo dia sem chamar ninguém de madrugada.
- 📊 **Entrego BI de ponta a ponta**: da modelagem ao painel que a diretoria realmente abre.
- ☁️ **Penso em nuvem**, custo e segurança desde o primeiro desenho.
- 🤝 **Falo a língua do negócio e a da engenharia**, e traduzo uma para a outra.

> 💡 *"Bom dado é aquele em que ninguém precisa perguntar de onde veio."*

---

## 🛠️ Caixa de ferramentas

| Área | O que uso |
|---|---|
| 🐍 **Linguagens** | Python · SQL |
| ☁️ **Cloud** | Google Cloud Platform (Cloud Storage e correlatos) |
| 🔄 **Engenharia de dados** | Ingestão · ETL/ELT · Orquestração · Modelagem dimensional |
| 📈 **BI e visualização** | Dashboards · Modelagem semântica · Storytelling com dados |
| 🧰 **Boas práticas** | Git · Versionamento · Documentação · Segurança de credenciais |

<!-- Ajuste esta tabela com as ferramentas que você realmente quer destacar para recrutadores. -->

---

## 🚀 Projetos

Cada pasta deste repositório é um projeto independente, com o próprio README, e cada um resolve um problema real. Clique para explorar.

| | Projeto | O que faz | Tecnologias | Status |
|:-:|---|---|---|:-:|
| ☁️ | **[GCP File Uploader](gcp-file-uploader/)** | App desktop que permite a qualquer pessoa enviar arquivos para um bucket do Google Cloud Storage com poucos cliques, sem console nem linha de comando. | `Python` `CustomTkinter` `Google Cloud Storage` | ✅ Pronto |
| ⛽ | **[Pipeline de Preços de Combustíveis no GCP](combustiveis-pipeline-gcp/)** | ELT dos dados abertos da ANP até o BigQuery, implementado em **Python (Cloud Run)** e em **PySpark (Dataproc)**, com comparativo de custo e tempo. | `Python` `PySpark` `Cloud Run` `Dataproc` `BigQuery` | 🔨 Em andamento |
| 🧱 | **[Modelagem Dimensional com dbt](combustiveis-dbt/)** | Esquema estrela, testes automáticos e documentação com linhagem sobre os dados de combustíveis. | `dbt` `BigQuery` `SQL` | 🚧 Planejado |
| 🎼 | **[Orquestração com Airflow e Composer](combustiveis-orquestracao/)** | O mesmo DAG rodando no Airflow local e no Cloud Composer, com retries, backfill e alertas. | `Airflow` `Cloud Composer` `Docker` | 🚧 Planejado |
| 📊 | **[Painel de Combustíveis no Power BI](combustiveis-bi-powerbi/)** | Painel sobre o modelo dimensional, que responde onde o combustível está mais caro e quando compensa o etanol. | `Power BI` `DAX` `BigQuery` | 🚧 Planejado |
| 🔜 | *Próximo projeto* | *Em breve…* | | 🚧 Planejado |

> ⛽ Os quatro projetos `combustiveis-*` formam **uma única história**: do arquivo cru da ANP ao painel de decisão.

---

## 🔎 Em destaque

### ☁️ [GCP File Uploader](gcp-file-uploader/)

> **Problema:** parceiros e clientes precisam enviar arquivos para o nosso Cloud Storage, mas não deveriam precisar de acesso ao console do GCP nem saber usar um terminal.
>
> **Solução:** uma interface simples, com upload via Service Account, que o usuário abre com dois cliques.

- 🖱️ Interface limpa em **CustomTkinter**, compacta e pensada para telas pequenas
- 🔐 **Segurança por padrão**: lista de extensões permitidas e bloqueio de executáveis
- 🧪 **Modo Simulação** para demonstrar o fluxo sem credencial nem bucket reais
- 📦 Empacotável em um **único `.exe`**, sem instalar Python na máquina de quem recebe

**Quer rodar?**

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r gcp-file-uploader/requirements.txt
python gcp-file-uploader/src/main.py
```

📖 [Ver documentação completa do projeto →](gcp-file-uploader/README.md)

---

## 🗂️ Como este repositório está organizado

```text
tanc-portifolio/
├── 📁 gcp-file-uploader/            ☁️  Upload de arquivos para o Google Cloud Storage
├── 📁 combustiveis-pipeline-gcp/    ⛽  ELT da ANP até o BigQuery (Python e PySpark)
├── 📁 combustiveis-dbt/             🧱  Modelagem dimensional com dbt
├── 📁 combustiveis-orquestracao/    🎼  Airflow local e Cloud Composer
├── 📁 combustiveis-bi-powerbi/      📊  Painel no Power BI
├── 📁 planos/                       🗺️  Roadmap e planos de desenvolvimento
└── 📄 README.md                     ← você está aqui
```

Cada projeto é autocontido: tem seu próprio `README.md`, suas dependências e instruções para rodar.

---

## 📬 Vamos conversar?

Se você chegou até aqui, provavelmente temos assuntos em comum: dados, nuvem e transformar informação em resultado. Fico feliz em trocar uma ideia sobre oportunidades, projetos ou só boas práticas de dados.

<div align="center">

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Conectar-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://br.linkedin.com/in/tancredo-alambert-14a45119)
[![E-mail](https://img.shields.io/badge/E--mail-Enviar_mensagem-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:t.alambert@gmail.com)

⭐ *Se algo aqui foi útil ou interessante, deixa uma estrela no repositório!*

</div>

<!--
COMO ADICIONAR UM NOVO PROJETO
1. Crie a pasta do projeto na raiz, com README.md próprio.
2. Na tabela "Projetos", troque a linha "Próximo projeto" por:
   | 📊 | **[Nome](pasta/)** | O que faz, em uma frase. | `Tech1` `Tech2` | ✅ Pronto |
   e mantenha uma nova linha "Próximo projeto" no fim.
3. Opcional: crie uma seção em "Em destaque" (problema, solução, 3-4 bullets).
4. Atualize o bloco "Como este repositório está organizado".
-->
