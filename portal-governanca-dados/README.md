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
| Portal, menu, telas de revisão, **confirmação antes de gravar**, aprovação e auditoria (com desfazer) | ✅ Real |
| Estado que sobrevive ao recarregar a página (auditoria, descrições gravadas e scans), em arquivo local | ✅ Real (no modo real vai para o BigQuery) |
| Revisão em escala: busca, filtros, "mostrar mais" e atualização por fragmentos, sem perder edições | ✅ Real |
| Erros com "o que aconteceu" e "o que fazer", sem deixar nada pela metade (com falhas simuláveis em Configurações) | ✅ Real |
| Mascaramento de dados sensíveis | ✅ Real, com testes |
| Avaliação das regras de qualidade (não nulo, único, faixa, conjunto, regex), teste em dado limpo e geração do YAML | ✅ Real, sobre amostras fictícias |
| Busca no catálogo, citação de fonte e recusa quando não há fonte | ✅ Real, mas lexical e sem LLM |
| Sugestões de metadados e propostas de regras | 🧪 Simulado (dados curados) |
| Profiling e *scans* no Dataplex, leitura do BigQuery, Gemini, embeddings | ⬜ A fazer (modo real) |
| Deploy no Cloud Run com login | ⬜ A fazer |

📋 Plano completo: [planos/08-portal-governanca-dados.md](../planos/08-portal-governanca-dados.md) (Fases 22 a 29). O protótipo antecipa a Fase 25 (portal) com dados simulados; o modo real segue as Fases 22 a 24, 26 e 27. O que falta está em [Próximos passos](#-próximos-passos).

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
1. **Metadados:** *Gerar sugestões* → note que a descrição escrita por pessoa e os itens de baixa confiança vêm desmarcados → edite um texto → *Revisar e gravar* → confira o resumo e *Gravar*. Teste também *Desfazer última aprovação*.
2. **Data Quality:** *Executar profiling* (sem ele a proposta de regras fica travada) → *Propor regras* (uma regra gera falso alarme no teste em dado limpo, vem destacada em vermelho e desmarcada) → *Aprovar* → *Publicar* (com confirmação) e veja as violações que as regras pegam.
3. **Catálogo:** pergunte *"O que significa o valor de compra?"* (responde com a fonte) e *"Qual a cotação do dólar hoje?"* (recusa). As descrições gravadas no passo 1 já aparecem nas respostas.
4. **Falhas:** em *Configurações → Simular falha do serviço*, escolha "Permissão negada" e tente gerar sugestões: o portal explica o que houve e o que fazer, e o estado fica intacto.
5. **Revisão em escala:** em Metadados, busque `sk_`, filtre por confiança ou use *Mostrar mais*. Edite um texto, filtre para escondê-lo e limpe o filtro: a edição continua lá.
6. **Início:** a *cobertura de documentação* sobe conforme você grava descrições. Recarregue a página: o estado continua. Para recomeçar, use *Configurações → Restaurar demonstração*.

## 📁 Estrutura atual

```text
portal-governanca-dados/
├── portal/
│   ├── app.py              navegação (Início · Governança · Sistema)
│   ├── ui.py               estado, cabeçalho, etapas, confirmação e CSS responsivo
│   ├── static/             logo e fontes (IBM Plex, licença OFL)
│   └── views/              inicio · metadados · qualidade · catalogo · configuracoes
├── src/governanca/
│   ├── core/               config · masking · audit · store (persistência) · errors (erros acionáveis)
│   ├── metadados/          sugestões e política de o que pré-selecionar
│   ├── qualidade/          regras, avaliação, profiling e YAML
│   ├── catalogo/           corpus, busca e resposta com citação
│   └── mock/               dados fictícios (substituídos no modo real)
├── .streamlit/config.toml  identidade visual: temas claro e escuro, tipografia, barra mínima
└── tests/                  lógica (mascaramento, regras, catálogo, estado) e fluxos da interface
```

## 🎨 Identidade visual

Ferramenta de revisão, sóbria e densa. Os valores ficam em [`.streamlit/config.toml`](.streamlit/config.toml), não espalhados pelo código.

| Escolha | Detalhe |
|---|---|
| Cor de ação | Verde-floresta `#0A5C20` (claro) e `#4FC47F` (escuro). Âmbar e vermelho só para alertas |
| Tipografia | IBM Plex Sans para texto e IBM Plex Mono para identificadores, servidas pelo próprio portal (sem depender de internet) |
| Temas | Claro e escuro explícitos, que seguem a preferência do sistema |
| Ícones | Material, em vez de emojis, para ficarem iguais em qualquer sistema |
| Responsivo | Em tela estreita, o cabeçalho da revisão some e cada linha ganha separação |

## ⏱️ Desempenho e acessibilidade

**Desempenho** (Edge, máquina de desenvolvimento, `fileWatcherType = none`):

| Medida | Resultado |
|---|---|
| Abrir uma página (navegador novo, 1ª visita) | 0,9 a 1,9 s |
| Abrir uma página (visita seguinte) | 0,3 a 0,9 s |
| Marcar ou desmarcar um item na revisão | ~0,5 s (era 1,8 s, antes de usar fragmentos) |

> **Produção:** defina `STREAMLIT_SERVER_FILE_WATCHER_TYPE=none`. Em ambientes em que as bibliotecas ficam fora do `site-packages` (instalação com `--target`, por exemplo), o Streamlit vigia centenas de arquivos a cada sessão e a página leva ~2,5 s a mais. No Cloud Run, mantenha também ao menos uma instância ativa (`min-instances=1`) para evitar a partida a frio.

**Acessibilidade** (axe-core 4.14, WCAG 2.x A/AA e boas práticas, nos temas claro e escuro): contraste de cores **0 ocorrências** (eram 73 no claro e 13 no escuro) e ordem de títulos correta. Restam apenas achados do próprio Streamlit, iguais em todas as páginas e fora do alcance do portal: landmarks da página, a lista da navegação lateral e o `aria-expanded` da barra lateral. Não foi feita auditoria com leitor de tela.

## 🧭 Próximos passos

O que falta para o portal sair do protótipo. Está dividido pelo que **depende do GCP** e pelo que **dá para fazer antes**, para o modo real entrar sem retrabalho. O plano completo, com fases e critérios de pronto, está em [planos/08-portal-governanca-dados.md](../planos/08-portal-governanca-dados.md).

### Pré-requisitos (a cargo de quem for conectar ao GCP)

- [ ] Projeto GCP com billing ativo e **alerta de orçamento** configurado.
- [ ] `gcloud` instalado e autenticado. Terraform e Docker também, para publicar no Cloud Run.
- [ ] APIs habilitadas: Dataplex, BigQuery, Vertex AI, Cloud Run, Artifact Registry.
- [ ] Região e cota do Vertex AI confirmadas para o modelo escolhido.
- [ ] Teto mensal de gasto com IA definido.
- [ ] Service Accounts separadas: leitura, escrita de metadados e criação de scans.

### Dá para fazer antes, sem GCP

| Item | O que entrega |
|---|---|
| Interface de armazenamento | Troca o arquivo local por uma interface, com implementação em SQLite. Permite testar edição simultânea de verdade, e o BigQuery entra depois como mais uma implementação |
| Papéis simulados | Seletor de usuário na simulação (leitor, revisor, aprovador) e as regras de quem pode gravar e publicar. No modo real, a identidade passa a vir do IAP |
| Adaptadores do modo real | Código de acesso ao Dataplex, ao BigQuery e ao Vertex AI atrás de interfaces, testado com dublês |
| Testes visuais no CI | Capturas automáticas com imagem de referência, em GitHub Actions, para pegar problemas de layout que os testes atuais não veem |
| Dockerfile | Escrito, mas o build só pode ser validado onde houver Docker |
| Capturas e GIF no README | Imagens do portal funcionando |

> **Atenção com os adaptadores:** escritos sem acesso ao serviço real, eles provam que a lógica do portal está certa, mas não que a API responde como imaginado. Os nomes e as respostas do Dataplex e do Vertex AI mudam, então espere ajustes ao conectar. O ideal é escrevê-los com o projeto GCP em mãos.

### Só com o GCP conectado

- [ ] **Modo real:** profiling e scans no Dataplex, leitura do BigQuery, Gemini e embeddings (Fases 22 a 24, 26 e 27 do plano).
- [ ] **Estado e auditoria no BigQuery**, compartilhados entre usuários, com controle de edição simultânea.
- [ ] **Login e aprovação por papéis via IAP.**
- [ ] **Deploy no Cloud Run:** `min-instances=1` e `STREAMLIT_SERVER_FILE_WATCHER_TYPE=none`, conforme a seção de desempenho.
- [ ] **Avaliação com dados e modelo de verdade:** qualidade das descrições contra descrições reais, e eficácia das regras com defeitos injetados.

### Pendências conhecidas

- [ ] Auditoria de acessibilidade com leitor de tela, e teste em outros navegadores (só o Edge foi usado).
- [ ] Achados do próprio Streamlit na acessibilidade (landmarks da página, lista da navegação lateral, `aria-expanded` da barra lateral). Avaliar reportar ao projeto.
- [ ] Depois de uma falha ao gerar sugestões, as antigas continuam na tela sem aviso de que estão desatualizadas.
- [ ] No celular, a tabela de cobertura da página inicial rola na horizontal.
- [ ] Sem controle de quem pode aprovar: qualquer pessoa que abre o portal aprova e publica.
- [ ] O estado atual (arquivo local) serve para uma pessoa só.

### Ordem sugerida

1. Interface de armazenamento e papéis simulados.
2. Testes visuais no CI e imagens no README.
3. Conectar ao GCP e implementar os adaptadores com o serviço real à mão.
4. Estado no BigQuery, IAP e deploy no Cloud Run.
5. Avaliação com dados e modelo reais.

## 🛡️ Princípios de projeto

- **Genérico por padrão:** nada específico de uma base de dados.
- **Biblioteca primeiro:** a lógica não depende da tela.
- **Propor, não aplicar:** nada é gravado ou criado sem aprovação.
- **Contexto antes de geração:** a IA recebe o que já se sabe sobre o dado.
- **Fidelidade acima de fluência:** o catálogo só afirma o que está nas fontes e sempre as cita.
- **Amostra mínima e mascarada:** só o necessário vai ao modelo.
- **Medir a qualidade:** proposta sem avaliação é opinião.
