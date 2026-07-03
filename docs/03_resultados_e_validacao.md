# Resultados & Validação

Este documento registra o resultado da execução real da pipeline (Bronze → Silver → Gold)
contra os 9 arquivos de `sources/`, validado localmente com PySpark + Delta Lake antes da
publicação (ver nota sobre o ambiente de desenvolvimento em
`02_arquitetura_e_modelagem.md`, seção 6).

## 1. Contagem de linhas por camada

| Entidade | Bronze (raw) | Silver (limpo) | Registros removidos/consolidados | Motivo |
|---|---:|---:|---:|---|
| produtos | 72 | 71 | 1 | Duplicata `P0006` (mantém versão "revisado") |
| canais | 8 | 7 | 1 | Duplicata `CH05` marcada como conflitante |
| regiões | 8 | 6 | 2 | `SE` duplicado + `sul`/`S` colapsam na mesma região |
| vendedores | 42 | 40 | 2 | Duplicatas `V004` (canal órfão) e `V008` (nome com sufixo) |
| clientes | 183 | 180 | 3 | Duplicatas `C0010`, `C0025`, `C0051` |
| pedidos (cabeçalho) | 403 | 400 | 3 | Duplicatas `O00011`, `O00081`, `O00121` |
| pedidos (itens) | 995 | 992 | 3 | Duplicatas por `(order_id, item_seq)`, incl. colisão com produto órfão `P8888` |
| entregas | 322 | 320 | 2 | Duplicatas `D00004` (pedido órfão) e `D00021` (data inválida) |
| ocorrências | 270 | 270 | 0 | Sem duplicatas na fonte |

| Tabela Gold | Linhas | Grão |
|---|---:|---|
| `dim_cliente` | 181 | 1 por cliente + 1 sintética (`CLIENTE_DESCONHECIDO`) |
| `dim_produto` | 72 | 1 por produto + 1 sintética (`PRODUTO_DESCONHECIDO`) |
| `dim_canal` | 7 | 1 por canal |
| `dim_regiao` | 6 | 1 por região |
| `dim_vendedor` | 40 | 1 por vendedor (já resolve canal e região) |
| `dim_data` | 433 | 1 por dia, 2025-01-01 a 2026-03-09 |
| `fact_pedidos` | 400 | 1 por pedido |
| `fact_pedidos_itens` | 992 | 1 por item de pedido |
| `fact_entregas` | 320 | 1 por entrega |
| `fact_ocorrencias` | 270 | 1 por ticket de atendimento |

## 2. Integridade referencial (100%, zero exceções)

Após a resolução de FK órfãs via dimensões sintéticas (`CLIENTE_DESCONHECIDO`,
`PRODUTO_DESCONHECIDO`):

- 0 valores nulos em `fact_pedidos.customer_id` ou `fact_pedidos_itens.product_id`
- 0 registros em `fact_pedidos`/`fact_pedidos_itens` cujo FK não existe na dimensão correspondente
- Os 8 pedidos que referenciavam o cliente placeholder `C9999` resolvem para `CLIENTE_DESCONHECIDO`
- O item que referenciava o produto órfão `P8888` foi identificado como duplicata do item com
  `P0047` e descartado ainda na Silver (não chega a virar um "produto desconhecido" na Gold)
- Nenhuma tabela fato tem chave de grão duplicada (`order_id` único em `fact_pedidos`,
  `(order_id, item_seq)` único em `fact_pedidos_itens`, `delivery_id` único em `fact_entregas`)

## 3. Suíte de testes automatizados

`tests/test_silver_quality_rules.py` (25 verificações) e
`tests/test_gold_integrity_and_business_questions.py` (17 verificações) — 42 verificações no
total, todas passando na última execução. Cobrem: contagens de deduplicação por tabela, casos
específicos de desempate documentados em `01_data_quality_findings.md`, parsing defensivo de
datas inválidas (incluindo o caso de rollover silencioso do Spark), padronização de enums, e
integridade referencial/granularidade da Gold.

Rodar localmente:
```bash
source scripts/env_spark.sh
python notebooks/01_bronze_ingestion.py
python notebooks/02_silver_transformation.py
python notebooks/03_gold_dimensional_model.py
python tests/test_silver_quality_rules.py
python tests/test_gold_integrity_and_business_questions.py
```

## 4. As tabelas Gold respondem às perguntas de negócio do case?

Consultas de exemplo rodadas diretamente sobre `gold.*` (sem nenhum retrabalho/tratamento
adicional), confirmando que o modelo atende ao pedido do case:

| Pergunta do negócio | Consulta | Resultado obtido nos dados do case |
|---|---|---|
| Receita líquida, qtd. pedidos, ticket médio | `SELECT sum(net_amount), count(*), avg(net_amount) FROM gold.fact_pedidos` | R$ 2.889.148,56 / 400 pedidos / R$ 7.222,87 |
| Taxa de cancelamento | `avg(is_cancelado::int) FROM gold.fact_pedidos` | 13,0% |
| Taxa de atraso (entre pedidos com entrega conhecida) | `avg(is_atrasado::int) WHERE is_atrasado IS NOT NULL` | 49,8% (de 285 pedidos com entrega rastreada) |
| Segmentação por região | `JOIN dim_vendedor GROUP BY regional_name` | Sul concentra a maior receita (R$ 1,20 mi / 161 pedidos) |
| Segmentação por canal | `JOIN dim_vendedor GROUP BY canal_nome` | Inside Sales lidera em receita (R$ 759,9 mil) |
| Segmentação por categoria de produto | `JOIN dim_produto GROUP BY category` | Assinatura lidera (R$ 1,97 mi) |
| Evolução temporal | `JOIN dim_data GROUP BY year, month` | 14 meses cobertos (jan/2025–mar/2026), sem buracos |
| Gargalo operacional (atraso por transportadora) | `GROUP BY carrier_name, carrier_mode` sobre `fact_entregas` | LogFast rodoviário tem o maior lead time médio (6,75 dias) |
| Cruzamento pedido × cliente × produto | `fact_pedidos_itens JOIN dim_cliente JOIN dim_produto` | 1 join direto por dimensão, sem tabela intermediária |

Todas as perguntas foram respondidas com **no máximo 1-2 joins diretos** a partir de uma
tabela fato, sem exigir tratamento ou consolidação adicional pelo analista — conforme
exigido na seção 1.2 do case.

## 5. Limitações conhecidas e sugestões de evolução

- `is_atrasado` em `fact_pedidos` depende de existir uma entrega correspondente rastreada
  (285 dos 400 pedidos); para os demais, o campo é `null` (não é possível inferir atraso sem
  uma data de conclusão conhecida) — decisão deliberada de não adivinhar.
- `metadata` em `atendimento_ocorrencias` foi preservado como veio da fonte (não flatten) por
  não haver, no escopo deste case, uma pergunta de negócio que dependesse do seu conteúdo;
  evolução futura: inspecionar e modelar essa coluna se vier a ser necessária.
- A divergência de estado do cliente `C0025` (RJ vs. Santa Catarina entre as duas versões
  duplicadas) foi resolvida pela regra genérica "mais recente vence" — o valor pode estar
  logicamente errado dado que a cidade (Niterói) é do RJ; não foi feita correção manual
  específica para não abrir precedente de regras caso-a-caso não documentadas.
- `dim_data` cobre apenas o intervalo efetivamente observado nas fontes; datas fora desse
  range (ex.: pedidos futuros hipotéticos) não teriam `date_key` correspondente até a próxima
  execução da pipeline.
