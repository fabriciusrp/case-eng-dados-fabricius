# Checklist de Conformidade com o Case

Auditoria item a item do PDF do case ("Case - Data Engineer.pdf") contra o que foi
efetivamente entregue neste repositório. Convenção: ✅ atendido · ⚠️ atendido parcialmente /
depende de ação externa · ❌ não atendido.

## Seção 2 — Objetivo do case

| Exigência | Status | Evidência |
|---|---|---|
| Explorar e entender estrutura das fontes brutas, formatos e conteúdos | ✅ | `exploration/profile_sources.py` + `docs/01_data_quality_findings.md` |
| Identificar relacionamentos e dependências entre os dados | ✅ | `docs/02_arquitetura_e_modelagem.md` §2 (mapeamento fonte→camada) e §4 (FKs entre fatos/dimensões) |
| Investigar qualidade das informações e definir estratégias de tratamento | ✅ | `docs/01_data_quality_findings.md` (achado → impacto → tratamento, por fonte) |
| Desenvolver transformações utilizando PySpark e Delta Tables | ✅ | `notebooks/01_bronze_ingestion.py`, `02_silver_transformation.py`, `03_gold_dimensional_model.py` — todas as tabelas em formato Delta |
| Estruturar a solução no Databricks | ⚠️ | Notebooks estão no formato nativo Databricks (`# Databricks notebook source`) e foram validados localmente com PySpark+Delta real (não simulado). **Ainda não foram executados dentro de um workspace Databricks Community Edition real** — recomendado antes da submissão final (ver seção "Gaps" abaixo) |
| Propor e construir modelo analítico final orientado ao consumo por BI | ✅ | `notebooks/03_gold_dimensional_model.py` — esquema estrela com 6 dimensões + 4 fatos |
| Documentar premissas, decisões técnicas e limitações encontradas | ✅ | `docs/01`, `02`, `03`, `04` |
| Organizar os artefatos de forma clara e rastreável | ✅ | Estrutura de pastas descrita no `README.md`; nomes de pasta (`sources/`) inclusive coincidem com o que o próprio PDF sugere |

## Seção 5 — Desafio proposto

| Exigência | Status | Evidência |
|---|---|---|
| Organizar a ingestão das fontes, considerando formatos e estruturas variados | ✅ | `01_bronze_ingestion.py` trata CSV `;`, CSV `,`, pipe-delimited, JSON aninhado, NDJSON e Excel individualmente |
| Investigar e tratar problemas de qualidade encontrados | ✅ | `docs/01` (achados) + `02_silver_transformation.py` (implementação) + `tests/test_silver_quality_rules.py` (regressão) |
| Consolidar entidades relacionadas vindas de diferentes fontes | ✅ | `dim_vendedor` consolida `vendedores` + `canais` + `regioes`; `fact_pedidos_itens` consolida `pedidos_itens` + `pedidos_cabecalho` |
| Definir a granularidade adequada das tabelas finais | ✅ | Grão de cada fato explicitado e justificado em `docs/02_arquitetura_e_modelagem.md` §4 |
| Separar claramente entidades principais (clientes, produtos, regiões, canais) dos eventos/transações (pedidos, entregas, ocorrências) | ✅ | Esquema estrela: `dim_*` vs `fact_*` |
| Disponibilizar tabelas prontas para consumo, nomes de colunas claros, relacionamentos bem definidos | ✅ | Colunas em português/inglês consistente por domínio, sem abreviações obscuras; FKs documentadas |
| Registrar validações, premissas e limitações | ✅ | `docs/03_resultados_e_validacao.md` §5 (limitações conhecidas) |

## Seção 6 — Entregáveis esperados

| Item | Status | Evidência |
|---|---|---|
| **6.1** Solução no Databricks (notebooks representando o fluxo completo) | ⚠️ | Notebooks prontos e validados localmente contra dados reais; falta rodar no workspace Databricks real (ação do usuário, sem acesso a um workspace neste ambiente) |
| **6.2** Transformações com Python/PySpark, podendo combinar com Spark SQL | ✅ | PySpark predominante; Spark SQL usado pontualmente (geração de `dim_data` via `sequence()`) |
| **6.3** Modelagem analítica final (granularidade, entidades, relacionamentos, premissas) | ✅ | `docs/02` §4 + `docs/03` §1 |
| **6.4** Qualidade de dados (investigação, tratamento, decisões documentadas e justificadas) | ✅ | `docs/01` completo, com "achado → impacto → tratamento" para as 9 fontes + 8 padrões transversais |
| **6.5** Publicação no GitHub em repositório público | ✅ | Publicado em [github.com/fabriciusrp/case-eng-dados-fabricius](https://github.com/fabriciusrp/case-eng-dados-fabricius) (público, branch `main`) |
| **6.6** Documentação técnica (visão geral, premissas, problemas/tratamento, decisões, validações, limitações, sugestões) | ✅ | `docs/01`, `02`, `03`, `04` cobrem todos os subitens pedidos |
| **6.7** Resumo executivo técnico (1-2 páginas OU 5-6 slides) | ✅ | `docs/04_resumo_executivo.md` (texto) **e** `docs/05_apresentacao.md` (slides) — as duas versões, mais completo que o mínimo pedido |

## Seção 9 — Formato de entrega

| Item | Status | Evidência |
|---|---|---|
| Link do repositório GitHub com notebooks, documentação técnica, resumo executivo | ✅ | https://github.com/fabriciusrp/case-eng-dados-fabricius |
| (Opcional) Diagrama da solução / desenho de camadas | ✅ | Diagramas em texto (ASCII) em `docs/02` §4 e `docs/05` slide 4 — suficiente para o nível de complexidade do case; pode ser substituído por um diagrama visual (draw.io/Excalidraw) se desejar um acabamento maior |
| (Opcional) Explicação de evoluções possíveis | ✅ | `docs/04_resumo_executivo.md` §"Próximos passos recomendados" |
| (Opcional) Observações sobre performance, governança ou reprocessamento | ✅ | `docs/02` §5 (limitações do Community Edition) e `docs/04` (Unity Catalog, incremental, DLT) |

---

## Gaps identificados (ação necessária do seu lado)

1. ~~Publicação no GitHub~~ — **concluído.** Repositório público em
   https://github.com/fabriciusrp/case-eng-dados-fabricius, branch `main`, 26 arquivos.
2. **Execução real no Databricks Community Edition (recomendado, seção 6.1).** Os notebooks
   foram validados linha a linha com PySpark + Delta Lake reais neste ambiente local (não é
   simulação), mas o case pede especificamente o Databricks Community Edition como ambiente
   de execução. Recomendo importar os 3 notebooks e os arquivos de `sources/` lá e rodar em
   sequência antes da entrega final, para confirmar que não há nenhuma particularidade do
   workspace (versão de runtime, permissões de schema) que exija ajuste. O `README.md` já tem
   o passo a passo.

Fora esse último ponto — que depende da sua conta do Databricks e de rodar a pipeline lá —,
**todos os demais itens do PDF estão atendidos**.
