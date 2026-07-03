# Arquitetura da Solução & Modelagem Analítica

## 1. Visão geral: Medallion Architecture

```
sources/ (9 arquivos brutos)
    │
    ▼
BRONZE  — ingestão 1:1, schema bruto preservado, sem lógica de negócio
    │      + colunas técnicas: _ingested_at, _source_file
    ▼
SILVER  — limpeza, padronização, deduplicação, conformação de tipos
    │      1 tabela silver por entidade-fonte, granularidade igual à origem
    ▼
GOLD    — modelo dimensional (fatos + dimensões), pronto para BI
```

Cada camada é uma pasta Delta Lake separada (schemas `bronze`, `silver`, `gold` no metastore
do Databricks Community Edition — ver seção 5 sobre a limitação do Unity Catalog).

### Por que Medallion aqui?
- **Bronze** preserva a fonte exatamente como chegou — se uma regra de tratamento na Silver
  estiver errada, refazemos sem re-extrair da fonte.
- **Silver** é onde a maior parte do trabalho de qualidade documentado em
  `01_data_quality_findings.md` é resolvida, uma vez, de forma reutilizável.
- **Gold** nunca faz limpeza — só modela (joins, agregações, granularidade). Isso mantém a
  camada de consumo simples e auditável.

---

## 2. Mapeamento fonte → Bronze → Silver

| # | Fonte (`sources/`) | Formato | Tabela Bronze | Tabela Silver | Grão da Silver |
|---|---|---|---|---|---|
| 1 | `cadastro_produtos_api_dump.json` | JSON aninhado | `bronze.produtos` | `silver.produtos` | 1 linha por `product_id` (deduplicado) |
| 2 | `erp_pedidos_cabecalho_2025.csv` | CSV `;` | `bronze.pedidos_cabecalho` | `silver.pedidos_cabecalho` | 1 linha por `order_id` (deduplicado) |
| 3 | `erp_pedidos_itens_2025.csv` | CSV `,` | `bronze.pedidos_itens` | `silver.pedidos_itens` | 1 linha por `(order_id, item_seq)` |
| 4 | `legado_regioes_pipe.txt` | Pipe `\|` | `bronze.regioes` | `silver.regioes` | 1 linha por `regional_code` normalizado |
| 5 | `vendedores.csv` | CSV `;` | `bronze.vendedores` | `silver.vendedores` | 1 linha por `seller_id` |
| 6 | `logistica_entregas.json` | JSON aninhado | `bronze.entregas` | `silver.entregas` | 1 linha por `delivery_id` |
| 7 | `atendimento_ocorrencias.ndjson` | NDJSON | `bronze.ocorrencias` | `silver.ocorrencias` | 1 linha por `ticket_id` |
| 8 | `comercial_canais.xlsx` | Excel | `bronze.canais` | `silver.canais` | 1 linha por `id_canal` |
| 9 | `crm_clientes_export.xlsx` | Excel | `bronze.clientes` | `silver.clientes` | 1 linha por `customer_id` |

**Leitura do Excel no Databricks Community:** sem cluster com lib `com.crealytics:spark-excel`
pré-instalada, a forma mais simples e portátil é ler com `pandas.read_excel` (motor `openpyxl`)
e converter para Spark DataFrame com `spark.createDataFrame(pandas_df)` — evita depender de
biblioteca externa Maven no cluster Community. Isso é documentado como decisão técnica, não
limitação.

**Por que o grão de `silver.pedidos_itens` é `(order_id, item_seq)` e não
`(order_id, item_seq, product_code)`:** a investigação de qualidade encontrou o pedido
`O00044`/item 3 duplicado com o **mesmo** `item_seq`, **mesma** quantidade/preço/total, mas
`product_code` diferente — uma linha aponta para `P0047` (produto real) e a outra para `P8888`
(não existe no catálogo). Tratar `product_code` como parte da chave faria as duas sobreviverem
como "itens distintos", inflando quantidade e receita. O grão correto é `(order_id, item_seq)`;
o desempate prefere o `product_code` que existe na dimensão de produtos.

---

## 3. Regras de tratamento aplicadas na Silver (resumo executável)

Todas as regras abaixo estão detalhadas com evidência em `01_data_quality_findings.md`. Aqui,
o resumo por *tipo* de regra, porque são implementadas como funções utilitárias compartilhadas:

| Função utilitária | O que faz | Onde é usada |
|---|---|---|
| `normalize_key(col)` | `upper(trim(col))` | Todas as chaves de negócio (`order_id`, `product_code`, `customer_id`, `seller_id`, `canal_id`, `regional_code`) |
| `standardize_enum(col, mapping)` | `lower(trim(col))` + de-para explícito (ex.: `EM_SEPARACAO`→`em_separacao`, `troca`→`exchange`) + nulo→`nao_informado` | `status_order`, `status_cliente`, `item_status`, `delivery_status`, `event_type`, `severity`, `status` (ocorrências), `status` (vendedores) |
| `parse_multi_format_date(col)` | Tenta `yyyy-MM-dd`, `dd/MM/yyyy`, `yyyy/MM/dd`, com/sem hora, nessa ordem; inválido → `null` + flag | `order_date`, `promised_date`, `hire_date`, `data_cadastro`, `shipped_at`, `delivered_at`, `created_at` |
| `parse_br_decimal(col)` | `regexp_replace(col, ',', '.')` antes do `cast(double)` | `gross_amount`, `unit_price` |
| `safe_cast_double(col)` | Cast com `try_cast`; valores como `"N/A"`, `"unknown"` viram `null` + flag `dq_*` | `list_price`, `cost` |
| `normalize_state(col)` | De-para estado→UF (tabela fixa, ~15 variações mapeadas) | `estado` (clientes), `destination.state` (entregas) |
| dedup por chave + "mais recente vence" | `ROW_NUMBER() OVER (PARTITION BY chave ORDER BY campo_data DESC)` = 1 | produtos, pedidos_cabecalho, clientes, vendedores, canais, entregas |

**Colunas de auditoria adicionadas em toda tabela Silver:**
`_dq_flags` (array de strings com os problemas encontrados naquela linha, ex.:
`["invalid_date:order_date", "unknown_cost"]`) — permite ao analista de BI (ou a nós, depois)
auditar quantos registros por tabela tiveram algum tipo de tratamento, sem esconder o problema.

---

## 4. Modelo Gold — dimensional (estrela)

### Princípio de granularidade

O case pede explicitamente para separar **entidades principais** (quem/o quê/onde) de
**eventos/transações** (o que aconteceu). Uso duas tabelas fato em granularidades diferentes
para pedidos — decisão deliberada, não redundância:

- **`fact_pedidos`** — grão **1 linha por pedido** (`order_id`). Usada para: quantidade de
  pedidos, taxa de cancelamento, taxa de atraso (`promised_date` vs data de faturamento/entrega),
  receita bruta/líquida por pedido, ticket médio.
- **`fact_pedidos_itens`** — grão **1 linha por item de pedido**. Usada para: receita por
  produto/categoria, quantidade vendida, mix de produtos.

Se usássemos só `fact_pedidos_itens` para contar pedidos, um pedido com 4 itens contaria 4x
sem `COUNT(DISTINCT)` — fonte comum de erro em dashboards. Duas tabelas no grão certo evitam
esse erro por construção, ao custo de ter `gross_amount`/`discount_amount`/`net_amount`
"duplicados" (mesmo valor) nas duas tabelas — trade-off aceito e documentado.

### Dimensões

| Tabela | Origem (Silver) | Chave | Observação |
|---|---|---|---|
| `dim_produto` | `silver.produtos` | `product_id` | Inclui produto `"PRODUTO_DESCONHECIDO"` sintético para resolver órfãos (`P8888`) |
| `dim_cliente` | `silver.clientes` | `customer_id` | Inclui cliente `"CLIENTE_DESCONHECIDO"` sintético (`C9999` e afins) |
| `dim_vendedor` | `silver.vendedores` join `silver.canais` join `silver.regioes` | `seller_id` | Já resolve canal e região do vendedor em atributos denormalizados, para simplificar consumo |
| `dim_regiao` | `silver.regioes` | `regional_code` | Standalone também, para joins diretos com entregas (`destination.state` → UF → região, quando aplicável) |
| `dim_canal` | `silver.canais` | `id_canal` | — |
| `dim_data` | Gerada (calendário) | `date_key` (yyyyMMdd) | Cobre do menor ao maior date encontrado em todas as fontes; colunas: ano, trimestre, mês, semana, dia da semana, flag fim de semana |

### Fatos

| Tabela | Origem (Silver) | Grão | FKs | Métricas-chave |
|---|---|---|---|---|
| `fact_pedidos` | `silver.pedidos_cabecalho` | 1 por pedido | `customer_id`, `seller_id`, `date_key` (order_date) | `gross_amount`, `discount_amount`, `net_amount`, `is_cancelado`, `is_atrasado` (calculado: entrega/faturamento após `promised_date`), `priority`, `source` (extraídos do JSON de `payment_details`) |
| `fact_pedidos_itens` | `silver.pedidos_itens` join `silver.pedidos_cabecalho` (para herdar `date_key`, `customer_id`, `seller_id`) | 1 por item | `product_id`, `order_id` (degenerada), + as mesmas FKs do pedido pai | `quantity`, `unit_price`, `total_item` |
| `fact_entregas` | `silver.entregas` | 1 por entrega | `order_id` (degenerada, pode ser órfã), `date_key` (shipped_at) | `cost`, `lead_time_dias` (calculado: `delivered_at - shipped_at`), `is_atrasado_entrega`, `carrier_name`, `carrier_mode`, `state` |
| `fact_ocorrencias` | `silver.ocorrencias` | 1 por ticket | `order_id`, `customer_code`, `date_key` (created_at) | `event_type`, `severity`, `status`, `is_aberto` |

### Diagrama lógico (texto)

```
                    dim_data
                       │
      ┌────────────────┼─────────────────┬───────────────┐
      │                │                 │               │
 fact_pedidos ──┬── dim_cliente     fact_entregas     fact_ocorrencias
      │         │                        │ (order_id degenerada)  │ (order_id, customer_code)
      │         └── dim_vendedor ── dim_canal
      │                  │
      │              dim_regiao
      │
fact_pedidos_itens ── dim_produto
      │
      └── (herda customer_id/seller_id/date_key de fact_pedidos via order_id)
```

---

## 5. Limitações conhecidas (Databricks Community Edition)

- **Sem Unity Catalog** → uso de `CREATE SCHEMA bronze/silver/gold` no metastore Hive padrão em
  vez de catálogos separados; nomenclatura `schema.tabela` (ex.: `gold.fact_pedidos`) simula a
  organização que o Unity Catalog daria nativamente.
- **Sem cluster job agendado persistente** no free tier → pipeline é pensado para execução
  manual/sequencial dos notebooks (Bronze → Silver → Gold), não como job orquestrado; isso é
  aceitável para o escopo do case (dado estático, não streaming).
- **Sem `spark-excel`** → leitura de `.xlsx` via `pandas` + `spark.createDataFrame`, conforme
  nota da seção 2.

## 6. Ambiente de desenvolvimento e validação (nota de transparência)

O ambiente local usado para desenvolver não tinha JVM/Java/Spark instalado inicialmente. Em
vez de prototipar só com pandas, foi montado um ambiente PySpark + Delta Lake real localmente
(JDK 17, Hadoop `winutils` para Windows, venv Python 3.11 — PySpark 3.5.x não suporta Python
3.14) especificamente para validar a lógica de transformação com Spark de verdade, não uma
aproximação. Essa validação local (42 testes automatizados, ver `tests/`) rodou sob o Spark
padrão (modo não-ANSI).

A pipeline foi **também executada de ponta a ponta em um workspace Databricks real**
(compute Serverless), com todas as 28 tabelas (9 Bronze + 9 Silver + 10 Gold) resultando nas
mesmas contagens de linha da validação local. Essa execução real revelou 5 diferenças de
ambiente que a validação local não expunha — a mais relevante sendo que o Databricks
Serverless roda em **modo ANSI por padrão**, no qual `to_timestamp()`/`cast()` lançam exceção
em entrada inválida em vez de retornar `null` silenciosamente. Todas as diferenças foram
corrigidas no código (não contornadas manualmente) e re-validadas localmente antes de cada
nova publicação. Lista completa em `docs/06_checklist_conformidade.md`, seção "Achados da
execução real no Databricks".
