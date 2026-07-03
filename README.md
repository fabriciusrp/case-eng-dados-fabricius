# Case Técnico — Engenheiro de Dados

Solução de engenharia de dados que transforma 9 fontes brutas heterogêneas (CSV, JSON
aninhado, NDJSON, TXT pipe-delimited e Excel) em uma base analítica confiável, organizada em
**Medallion Architecture** (Bronze → Silver → Gold) e modelada em **esquema estrela** para
consumo direto por um Analista de BI, usando **PySpark** e **Delta Lake** no
**Databricks Community Edition**.

## Sumário executivo

- **Fontes:** cadastro de produtos, pedidos (cabeçalho + itens), vendedores, canais, regiões,
  clientes (CRM), entregas e ocorrências de atendimento — ver `sources/`.
- **Achados de qualidade:** duplicatas por chave de negócio, casing inconsistente, datas em
  3+ formatos (incluindo datas logicamente inválidas), separador decimal BR, FKs órfãs,
  vocabulário fragmentado (PT/EN, maiúsculas/minúsculas) — catalogados e tratados em toda a
  camada Silver. Ver `docs/01_data_quality_findings.md`.
- **Modelo final:** 6 dimensões + 4 fatos em grãos explícitos (`fact_pedidos` por pedido,
  `fact_pedidos_itens` por item — grãos separados deliberadamente para não duplicar contagem
  de pedidos). Ver `docs/02_arquitetura_e_modelagem.md`.
- **Validação:** 42 testes automatizados (dedup, parsing de datas, integridade referencial,
  granularidade, respostas às perguntas de negócio do case) — todos passando contra os dados
  reais. Ver `docs/03_resultados_e_validacao.md`.
- **Resumo executivo (1-2 páginas):** `docs/04_resumo_executivo.md` — também disponível em
  formato de slides em `docs/05_apresentacao.md`.
- **Conformidade com o case:** `docs/06_checklist_conformidade.md` audita cada exigência do
  PDF do case contra o que foi entregue, com gaps pendentes explicitados.

## Estrutura do repositório

```
├── sources/                          Fontes brutas (não editadas)
├── notebooks/                        Pipeline PySpark, no formato Databricks notebook
│   ├── utils.py                      Funções de tratamento reutilizáveis (Silver)
│   ├── 01_bronze_ingestion.py        Ingestão 1:1 das 9 fontes
│   ├── 02_silver_transformation.py   Limpeza, padronização, deduplicação
│   └── 03_gold_dimensional_model.py  Dimensões + fatos (esquema estrela)
├── tests/                            Testes automatizados contra dados reais
│   ├── test_silver_quality_rules.py
│   └── test_gold_integrity_and_business_questions.py
├── exploration/
│   └── profile_sources.py            Script de investigação inicial (pandas) — rastreabilidade
├── docs/
│   ├── 01_data_quality_findings.md   Achados de qualidade por fonte + tratamento proposto
│   ├── 02_arquitetura_e_modelagem.md Medallion + modelo dimensional + decisões
│   ├── 03_resultados_e_validacao.md  Contagens finais, testes, respostas de negócio
│   └── 04_resumo_executivo.md        Resumo executivo técnico (1-2 páginas)
├── scripts/
│   └── env_spark.sh                  Configuração de ambiente para rodar localmente (Windows)
└── requirements.txt
```

## Como executar

### Opção A — Databricks Community Edition (ambiente de destino)

1. Crie um workspace no [Databricks Community Edition](https://community.cloud.databricks.com/).
2. Suba os 9 arquivos de `sources/` para um volume/DBFS (ex.: `dbfs:/FileStore/case_de/sources/`).
3. Importe os arquivos de `notebooks/` (aceitam import direto como notebooks Databricks — o
   cabeçalho `# Databricks notebook source` e os separadores `# COMMAND ----------` já estão
   no formato esperado).
4. Rode nesta ordem: `01_bronze_ingestion.py` → `02_silver_transformation.py` →
   `03_gold_dimensional_model.py`. Cada um cria seu schema (`bronze`, `silver`, `gold`) no
   metastore do workspace automaticamente.
5. Consulte as tabelas finais em `gold.*` a partir de qualquer notebook SQL ou do Databricks SQL.

### Opção B — Execução local (desenvolvimento/validação)

Requer Python 3.11 (PySpark 3.5.x não suporta versões mais novas) e um JDK 17.

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt   # Windows
# .venv/bin/pip install -r requirements.txt     # Linux/Mac

source scripts/env_spark.sh   # define JAVA_HOME/HADOOP_HOME/etc — ajuste os caminhos para o seu ambiente
export CASE_DE_SOURCE_PATH="file:///$(pwd)/sources"
export CASE_DE_WAREHOUSE_DIR="file:///$(pwd)/spark-warehouse"
export CASE_DE_METASTORE_DIR="$(pwd)/metastore_db"

python notebooks/01_bronze_ingestion.py
python notebooks/02_silver_transformation.py
python notebooks/03_gold_dimensional_model.py
python tests/test_silver_quality_rules.py
python tests/test_gold_integrity_and_business_questions.py
```

No Windows, `winutils.exe` + `hadoop.dll` (compatíveis com Hadoop 3.3.x, ex.:
[cdarlint/winutils](https://github.com/cdarlint/winutils)) precisam estar em `HADOOP_HOME/bin`
— detalhe de ambiente documentado em `docs/02_arquitetura_e_modelagem.md`, seção 6.

## Documentação completa

| Documento | Conteúdo |
|---|---|
| [`docs/01_data_quality_findings.md`](docs/01_data_quality_findings.md) | Achado → impacto → tratamento, por fonte, com evidência |
| [`docs/02_arquitetura_e_modelagem.md`](docs/02_arquitetura_e_modelagem.md) | Medallion, mapeamento fonte→camada, modelo estrela, limitações do Community Edition |
| [`docs/03_resultados_e_validacao.md`](docs/03_resultados_e_validacao.md) | Contagens reais, suíte de testes, respostas às perguntas de negócio do case |
| [`docs/04_resumo_executivo.md`](docs/04_resumo_executivo.md) | Resumo executivo técnico (decisões, desafios, próximos passos) |
| [`docs/05_apresentacao.md`](docs/05_apresentacao.md) | Versão em slides do resumo executivo (Marp-compatível) |
| [`docs/06_checklist_conformidade.md`](docs/06_checklist_conformidade.md) | Auditoria requisito-a-requisito do PDF do case vs. entrega |
