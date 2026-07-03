<!--
Apresentação em formato Markdown (compatível com Marp, reveal.js ou export direto para PDF/PPTX
via pandoc). Cada "---" separa um slide. Ver docs/04_resumo_executivo.md para a versão em texto
corrido (1-2 páginas), conforme oferecido pelo case (seção 6.7).

Exportar para PDF (exemplo com Marp CLI):
    npx @marp-team/marp-cli docs/05_apresentacao.md -o docs/05_apresentacao.pdf
-->

# Case Técnico — Engenheiro de Dados
### De fontes brutas heterogêneas a uma base analítica confiável

**Medallion Architecture (Bronze → Silver → Gold) em PySpark + Delta Lake**
Databricks Community Edition

---

## 1. O que foi construído

- Pipeline completa **Bronze → Silver → Gold** para 9 fontes brutas heterogêneas
  (CSV `;`/`,`, JSON aninhado, NDJSON, TXT pipe-delimited, Excel)
- Modelo dimensional final: **6 dimensões + 4 fatos** em grãos explícitos
- **42 testes automatizados** validando regras de qualidade contra os dados reais
- Documentação completa: achados de qualidade, arquitetura, resultados, resumo executivo

**Resultado:** zero valores nulos ou órfãos em chave estrangeira nas tabelas finais;
todas as perguntas de negócio do case respondidas com no máximo 1-2 joins diretos.

---

## 2. Principais decisões técnicas

| Decisão | Por quê |
|---|---|
| Duas fatos de pedido (`fact_pedidos` por pedido, `fact_pedidos_itens` por item) | Evita `COUNT(DISTINCT)` para contar pedidos — erro comum quando só existe a tabela no grão de item |
| FK órfã nunca descarta o fato (resolve para `*_DESCONHECIDO`) | Descartar inflaria artificialmente taxas de cancelamento/atraso |
| Dedup por critério de conteúdo, não "ordem do arquivo" | `monotonically_increasing_id()` não é confiável após round-trip em Delta — descoberto e corrigido com testes |
| Validação round-trip no parser de data | Spark faz *rollover* silencioso em datas inválidas (`"2025-13-40"` → uma data errada, não `null`) |
| Excel via pandas + `spark.createDataFrame` | Evita depender de lib Maven externa (`spark-excel`) no cluster Community |

---

## 3. Principais desafios encontrados

- **8 padrões de problema de qualidade** repetidos nas 9 fontes: casing inconsistente,
  enums fragmentados em PT/EN, datas em 3+ formatos, decimal BR, duplicidade de registro,
  FK órfã, sentinela textual (`"N/A"`, `"unknown"`), estado/UF com dezenas de grafias
  → resolvidos com um conjunto pequeno de funções reutilizáveis, não tratamento pontual
- **Ambiente local sem Java/Spark pré-instalado** — montado do zero (JDK, Hadoop winutils,
  venv Python 3.11) só para viabilizar validação da lógica contra dados reais antes da entrega
- **Bugs só visíveis testando o *conteúdo* do resultado**, não só a contagem de linhas:
  rollover silencioso de data inválida e não-determinismo de `monotonically_increasing_id()`

---

## 4. Visão geral do modelo final

```
dim_cliente ──┐                     ┌── dim_produto
              ├── fact_pedidos ─────┤
dim_vendedor ─┤        │            └── fact_pedidos_itens
              │        │
dim_canal ────┘   fact_entregas (order_id degenerada)

dim_regiao ─── dim_vendedor     fact_ocorrencias (order_id, customer_id)

dim_data ─── (todas as fatos, via date_key)
```

| Fato | Linhas | Grão |
|---|---:|---|
| `fact_pedidos` | 400 | 1 por pedido |
| `fact_pedidos_itens` | 992 | 1 por item de pedido |
| `fact_entregas` | 320 | 1 por entrega |
| `fact_ocorrencias` | 270 | 1 por ticket |

---

## 5. Prova de conceito: perguntas do case respondidas

| Pergunta | Resultado |
|---|---|
| Receita líquida / qtd. pedidos / ticket médio | R$ 2,89 mi / 400 / R$ 7.222,87 |
| Taxa de cancelamento | 13,0% |
| Taxa de atraso (pedidos com entrega rastreada) | 49,8% |
| Região com maior receita | Sul (R$ 1,20 mi) |
| Categoria líder | Assinatura (R$ 1,97 mi) |
| Maior gargalo logístico | LogFast rodoviário (6,75 dias de lead time médio) |

Todas obtidas com consultas diretas sobre `gold.*`, sem retrabalho.

---

## 6. Próximos passos recomendados

1. **Orquestração:** migrar de execução manual sequencial para Databricks Workflows
2. **Carga incremental:** trocar `overwrite` por `MERGE` com watermark de `updated_at`
3. **Unity Catalog:** governança e lineage nativos ao sair do Community Edition
4. **Modelar `metadata` de ocorrências:** hoje preservado cru, sem pergunta de negócio que o exija ainda
5. **Qualidade contínua:** evoluir os testes atuais para Delta Live Tables com expectations declarativas

**Repositório:** notebooks, testes e documentação completa disponíveis no GitHub.
