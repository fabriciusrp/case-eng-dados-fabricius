# Resumo Executivo Técnico

**Case:** Engenheiro de Dados — construção de base analítica a partir de fontes brutas heterogêneas
**Autor:** Fabrícius Renó
**Data:** Julho/2026

---

## O que foi construído

Uma pipeline de engenharia de dados em **Medallion Architecture** (Bronze → Silver → Gold),
implementada em **PySpark + Delta Lake** para rodar no **Databricks Community Edition**, que
transforma 9 fontes brutas (2 CSV, 3 JSON/NDJSON, 1 TXT pipe-delimited, 2 Excel) — contendo
clientes, produtos, vendedores, canais, regiões, pedidos, entregas e ocorrências de
atendimento — em um **modelo dimensional (esquema estrela)** pronto para consumo por um
Analista de BI: 6 dimensões, 4 fatos em grãos explícitos, zero valores nulos ou órfãos em
chave estrangeira.

A entrega inclui: os notebooks da pipeline, 42 testes automatizados que validam as regras de
tratamento contra os dados reais, e documentação cobrindo cada achado de qualidade com sua
justificativa de tratamento.

## Principais decisões técnicas

1. **Duas fatos de pedido em grãos diferentes** (`fact_pedidos` por pedido, `fact_pedidos_itens`
   por item), em vez de uma única tabela. Motivo: contar pedidos a partir de uma tabela no
   grão de item exigiria `COUNT(DISTINCT)` — fonte comum de erro em dashboards. Grãos
   separados eliminam essa classe de erro por construção, ao custo de duplicar (mesmo valor)
   `gross_amount`/`net_amount` nas duas tabelas.

2. **Nunca descartar um fato por causa de FK órfã.** Produto, cliente e canal inexistentes
   (`P8888`, `C9999`, `CH99`) resolvem para uma linha sintética `*_DESCONHECIDO` na dimensão,
   via left join. O pedido/item/entrega correspondente continua contável — descartar a linha
   inflaria artificialmente as taxas de cancelamento/atraso ao remover volume real de pedidos.

3. **Deduplicação por regra de conteúdo, não por "primeira linha do arquivo".** Testado e
   revertido: usar `monotonically_increasing_id()` como critério de desempate não é confiável
   depois que os dados passam por um round-trip em Delta (a ordem física das partições não
   preserva a ordem do arquivo original). A correção final usa critérios de conteúdo
   explícitos — ex.: preferir e-mail em formato válido, preferir FK que existe na dimensão,
   preferir o registro com `updated_at` mais recente — documentados caso a caso.

4. **Validação de round-trip para datas.** O parser de data padrão do Spark (`to_timestamp`)
   faz *rollover* silencioso de valores fora do intervalo (ex.: `"2025-13-40"` não gera erro
   nem `null` — vira uma data "corrigida" e errada). A pipeline reformata a data parseada de
   volta ao padrão original e só aceita o resultado se a string bater exatamente; caso
   contrário, `null`.

5. **Leitura de Excel via pandas + `spark.createDataFrame`**, não `spark-excel`, para não
   depender de uma lib Maven externa no cluster Community (nem sempre disponível/atualizável
   nesse tier gratuito).

## Principais desafios encontrados

- **Qualidade dos dados:** 8 padrões de problema se repetem nas 9 fontes (casing de chaves,
  enums fragmentados em PT/EN, datas em 3+ formatos, decimal BR, duplicidade de registro,
  FK órfã, sentinela textual em campo numérico, estado/UF com dezenas de grafias) — resolvidos
  com um conjunto pequeno de funções utilitárias reaproveitadas em todas as 9 tabelas Silver,
  em vez de tratamento pontual por tabela.
- **Ambiente de desenvolvimento local sem Java/Spark pré-instalado:** exigiu montar o
  ambiente do zero (JDK, `winutils`/`hadoop.dll` para Windows, venv Python 3.11 — PySpark 3.5
  não suporta Python 3.14) antes de conseguir validar qualquer lógica de tratamento contra
  dados reais. Documentado para transparência; não afeta a portabilidade da entrega para o
  Databricks, que já provê Spark nativamente.
- **Bugs sutis só visíveis com dados reais e testes automatizados:** o rollover silencioso de
  datas inválidas e a não-confiabilidade de `monotonically_increasing_id()` como critério de
  ordenação só apareceram ao escrever testes que verificam o *conteúdo* do resultado (não só
  a contagem de linhas) — reforça o valor de testar contra casos concretos documentados na
  investigação de qualidade, não só "rodar sem erro".

## Visão geral do modelo final

```
dim_cliente ─┐                    ┌─ dim_produto
             ├─ fact_pedidos ─────┤
dim_vendedor ┤        │           └─ fact_pedidos_itens
             │        │
dim_canal ───┘   fact_entregas (order_id degenerada)
             
dim_regiao ──── dim_vendedor    fact_ocorrencias (order_id, customer_id)
             
dim_data ──── (todas as fatos, via date_key)
```

- `fact_pedidos` (400 linhas, grão = 1/pedido): receita, cancelamento, atraso, prioridade.
- `fact_pedidos_itens` (992 linhas, grão = 1/item): quantidade, preço, categoria de produto.
- `fact_entregas` (320 linhas, grão = 1/entrega): custo, lead time, transportadora, atraso.
- `fact_ocorrencias` (270 linhas, grão = 1/ticket): tipo, severidade, status de atendimento.

## Próximos passos recomendados

1. **Orquestração:** hoje os 3 notebooks rodam manualmente em sequência; evoluir para
   Databricks Workflows com dependência explícita entre tarefas e alertas de falha.
2. **Incremental em vez de full overwrite:** a pipeline atual reprocessa tudo a cada execução
   (`mode("overwrite")`), adequado para o volume do case; para produção, migrar para MERGE
   incremental usando `_ingested_at`/`last_update` como watermark.
3. **Unity Catalog:** ao sair do Community Edition, migrar `bronze`/`silver`/`gold` de schemas
   soltos no Hive metastore para catálogos Unity Catalog com governança/lineage nativos.
4. **Modelar `metadata` de ocorrências:** hoje preservado cru por não haver pergunta de
   negócio que dependesse dele; inspecionar estrutura real e expandir se necessário.
5. **Monitoramento de qualidade contínuo:** hoje os testes rodam sob demanda; evoluir para
   Delta Live Tables (DLT) com expectations declarativas, rodando a cada execução da pipeline
   automaticamente.
