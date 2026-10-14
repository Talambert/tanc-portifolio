# 9️⃣ Plano — `extracao-documentos-ia`

**Objetivo:** pipeline que recebe **PDFs e planilhas sem padrão fixo**, usa **Gemini (Vertex AI)** para **extrair os dados em um esquema definido**, **valida cada campo** com regras independentes do modelo e carrega no **BigQuery**. O que não for confiável vai para **quarentena** com o motivo, para revisão humana.

**Pré-requisitos:** nenhum projeto anterior é obrigatório. Opcionalmente, alimenta o projeto 1 quando a fonte não for CSV.

**Entrega final:** relatório de **acurácia por campo** em documentos de referência com gabarito, com destaque para a métrica que mais importa: **erros que passaram direto para a base sem revisão**.

## Dados de exemplo (decisão importante)

Para ter **gabarito exato** e **nenhum dado de empresa**, os documentos de teste são **gerados por um script** a partir de dados fictícios, por exemplo um "relatório mensal de vendas de uma distribuidora fictícia" e "notas de entrega fictícias". Assim o valor correto de cada campo é conhecido de antemão. Se quiser, complementa-se com poucos PDFs públicos reais.

O gerador produz **variações** de propósito: layouts diferentes, colunas em outra ordem, página rotacionada, imagem de baixa qualidade, campos ausentes, valores com formato diferente (`1.234,56` × `1234.56`) e um texto malicioso embutido no documento.

## Estrutura da pasta

```text
extracao-documentos-ia/
├── generator/                 # cria documentos fictícios com gabarito
│   ├── make_documents.py
│   └── variations.yaml
├── extractor/
│   ├── schemas.py             # esquemas Pydantic versionados
│   ├── extract.py             # chamada ao Gemini com saída estruturada
│   ├── prompts/
│   └── validators.py          # regras independentes do modelo
├── pipeline/
│   ├── process_document.py    # orquestra: extrair → validar → rotear
│   ├── load_bigquery.py
│   └── review_queue.py        # quarentena e revisão
├── evals/
│   ├── ground_truth/          # gabaritos
│   └── run_evals.py
├── app/                       # tela simples de revisão humana
├── infra/
└── README.md
```

## Desenho

```text
 PDF / planilha (Cloud Storage)
        │ evento
        ▼
 Cloud Run ──▶ Gemini (Vertex AI), saída no esquema ──▶ validação
                                                           │
                          ┌────────────────────────────────┴───────────────┐
                          ▼                                                ▼
              aprovado → BigQuery (extraidos)          quarentena (motivo) → revisão humana → BigQuery
```

## Validação em camadas (o diferencial do projeto)

| Camada | Exemplos |
|---|---|
| **Esquema** | Tipos corretos, campos obrigatórios, listas controladas |
| **Faixa e formato** | Datas plausíveis, valores positivos, CNPJ com dígito verificador válido |
| **Consistência entre campos** | Soma dos itens = total do documento; data de vencimento depois da emissão |
| **Evidência** | Cada campo traz o trecho e a página de origem, e o trecho **existe** no documento |
| **Concordância** | Duas extrações independentes discordam? Vai para quarentena |
| **Duplicidade** | Mesmo documento (hash) ou mesma chave de negócio já carregados |

Um campo só é aprovado se passar por **todas** as camadas aplicáveis. A "confiança" não é só o que o modelo diz: vem das verificações.

## Fases

### Fase 18 — Gerador e esquema
- [ ] `make_documents.py` gerando documentos fictícios (PDF e planilha) **com gabarito em JSON**.
- [ ] Variações de layout e de qualidade, mais um documento com texto malicioso.
- [ ] Esquemas Pydantic versionados.
- ✅ **Pronto quando:** existe um conjunto de ~60 documentos com gabarito, cobrindo todas as variações.

### Fase 19 — Extração
- [ ] Extração com Gemini no Vertex AI, entrada de PDF e imagem, **saída em esquema** (JSON validado).
- [ ] Evidência por campo (trecho e página).
- [ ] Execução local sobre o conjunto de teste.
- ✅ **Pronto quando:** o pipeline extrai o conjunto inteiro e grava os resultados em arquivo, para medir.

### Fase 20 — Validação, quarentena e carga
- [ ] Camadas de validação em `validators.py`, cada uma com testes.
- [ ] Roteamento: aprovados → BigQuery; duvidosos → quarentena com o motivo.
- [ ] Tela simples de revisão (aprovar, corrigir ou rejeitar).
- [ ] Idempotência por hash do documento e rastreabilidade (documento, página, versão do esquema, modelo usado).
- ✅ **Pronto quando:** reprocessar um documento não duplica linhas e todo item em quarentena tem motivo.

### Fase 21 — Avaliação e acabamento
- [ ] `run_evals.py` comparando extração × gabarito.
- [ ] Comparativo: modelo menor × maior, uma extração × duas com concordância, com × sem validação.
- [ ] Gatilho por evento do Cloud Storage (Eventarc) para processamento automático.
- [ ] CI com testes dos validadores e um subconjunto dos documentos.
- [ ] README com diagrama, resultados e GIF.
- ✅ **Pronto quando:** o README tem a tabela de acurácia, custo e taxa de erro aprovado indevidamente, por configuração.

## Avaliação

| Métrica | Por que importa |
|---|---|
| **Acurácia por campo** (exatidão, precisão e cobertura) | Qualidade geral da extração |
| **Erro aprovado indevidamente** | Campos errados que chegaram à base sem revisão. É **a métrica crítica**, e a meta é perto de zero |
| **Precisão da quarentena** | Dos itens enviados para revisão, quantos estavam realmente errados |
| **Taxa de automação** | Fração dos documentos aprovados sem intervenção humana |
| **Robustez** | Texto malicioso no documento não altera o resultado |
| **Custo e latência por documento** | Viabilidade em escala |

Há uma troca explícita: quanto mais rigorosa a validação, menos erros passam, porém mais itens vão para revisão. O relatório mostra essa curva e justifica o ponto escolhido.

## Segurança

- **O documento é dado:** instruções escritas dentro do PDF nunca são seguidas (caso adversarial incluído nos testes).
- Somente documentos fictícios ou públicos. Nenhum dado pessoal ou de empresa.
- Sem permissão de escrita fora do dataset de destino e do bucket de quarentena.

## Riscos

| Risco | Mitigação |
|---|---|
| Valor plausível, porém errado, passando despercebido | Validações independentes do modelo, concordância entre duas extrações, métrica de erro aprovado |
| Documentos de baixa qualidade | Detectar e enviar para quarentena em vez de extrair |
| Custo por documento | Modelo menor por padrão, segunda extração só quando necessário |
| Esquema que muda com o tempo | Esquemas versionados e versão gravada em cada linha |
| Mudança de nomes e modelos no Vertex AI | Modelo por configuração e verificação na documentação oficial |
| Conjunto de teste muito "limpo" | Variações deliberadas e alguns documentos públicos reais |

## Definição de pronto

- [ ] Gerador de documentos fictícios com gabarito e variações.
- [ ] Extração com saída estruturada e evidência por campo.
- [ ] Validação em camadas, quarentena com motivo e revisão humana.
- [ ] Relatório com acurácia, taxa de erro aprovado indevidamente, custo e latência.
- [ ] Reprocessamento idempotente e rastreabilidade completa.
