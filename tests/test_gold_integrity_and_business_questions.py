"""
Testes de integridade referencial e de "capacidade de resposta" da camada Gold às perguntas
de negócio listadas no case (seção 1.2 / 6.3 do PDF): receita líquida, quantidade de pedidos,
ticket médio, taxas de cancelamento e atraso, segmentação por região/canal/categoria/período.

Uso: ver cabeçalho de tests/test_silver_quality_rules.py (mesmo procedimento, rodar depois de
notebooks/03_gold_dimensional_model.py).
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "notebooks"))
from utils import get_or_create_spark  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

spark = get_or_create_spark()

failures = []


def check(description, condition):
    status = "OK  " if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


# ---------------------------------------------------------------------------
# Integridade referencial — nenhuma FK deve resultar em null ou órfão após join
# ---------------------------------------------------------------------------

fact_pedidos = spark.table("gold.fact_pedidos")
fact_pedidos_itens = spark.table("gold.fact_pedidos_itens")
dim_cliente = spark.table("gold.dim_cliente")
dim_produto = spark.table("gold.dim_produto")

check(
    "fact_pedidos.customer_id nunca é null",
    fact_pedidos.filter(F.col("customer_id").isNull()).count() == 0,
)
check(
    "fact_pedidos_itens.product_id nunca é null",
    fact_pedidos_itens.filter(F.col("product_id").isNull()).count() == 0,
)
check(
    "todo customer_id de fact_pedidos existe em dim_cliente (incl. CLIENTE_DESCONHECIDO)",
    fact_pedidos.join(dim_cliente, "customer_id", "left_anti").count() == 0,
)
check(
    "todo product_id de fact_pedidos_itens existe em dim_produto (incl. PRODUTO_DESCONHECIDO)",
    fact_pedidos_itens.join(dim_produto, "product_id", "left_anti").count() == 0,
)
check(
    "cliente placeholder C9999 foi resolvido para CLIENTE_DESCONHECIDO, não aparece cru",
    fact_pedidos.filter("customer_id = 'C9999'").count() == 0,
)
check(
    "produto órfão P8888 foi resolvido para PRODUTO_DESCONHECIDO, não aparece cru",
    fact_pedidos_itens.filter("product_id = 'P8888'").count() == 0,
)
check(
    "8 pedidos com cliente placeholder C9999 resolvem para CLIENTE_DESCONHECIDO",
    fact_pedidos.filter("customer_id = 'CLIENTE_DESCONHECIDO'").count() >= 8,
)

# ---------------------------------------------------------------------------
# Granularidade — nenhuma tabela fato deve ter chave de negócio duplicada no seu grão
# ---------------------------------------------------------------------------

check(
    "fact_pedidos: order_id é único (grão = 1 por pedido)",
    fact_pedidos.count() == fact_pedidos.select("order_id").distinct().count(),
)
check(
    "fact_pedidos_itens: (order_id, item_seq) é único (grão = 1 por item)",
    fact_pedidos_itens.count()
    == fact_pedidos_itens.select("order_id", "item_seq").distinct().count(),
)
check(
    "fact_entregas: delivery_id é único (grão = 1 por entrega)",
    spark.table("gold.fact_entregas").count()
    == spark.table("gold.fact_entregas").select("delivery_id").distinct().count(),
)

# ---------------------------------------------------------------------------
# Perguntas de negócio do case — o modelo deve conseguir responder sem retrabalho
# ---------------------------------------------------------------------------

kpis = fact_pedidos.agg(
    F.sum("net_amount").alias("receita_liquida"),
    F.count("*").alias("qtd_pedidos"),
    F.avg("net_amount").alias("ticket_medio"),
    (F.sum(F.col("is_cancelado").cast("int")) / F.count("*") * 100).alias("pct_cancelado"),
).first()

check("receita líquida agregada é calculável e positiva", kpis["receita_liquida"] > 0)
check("quantidade de pedidos bate com o grão da fact_pedidos", kpis["qtd_pedidos"] == fact_pedidos.count())
check("ticket médio é calculável e positivo", kpis["ticket_medio"] > 0)
check("taxa de cancelamento é calculável (0-100%)", 0 <= kpis["pct_cancelado"] <= 100)

receita_por_regiao = (
    fact_pedidos.join(spark.table("gold.dim_vendedor"), "seller_id", "left")
    .groupBy("regional_name")
    .agg(F.sum("net_amount").alias("receita"))
)
check(
    "receita é segmentável por região sem retrabalho (1 join, 1 groupBy)",
    receita_por_regiao.count() > 1,
)

receita_por_categoria = (
    fact_pedidos_itens.join(dim_produto, "product_id", "left")
    .groupBy("category")
    .agg(F.sum("total_item").alias("receita"))
)
check(
    "receita é segmentável por categoria de produto sem retrabalho",
    receita_por_categoria.count() > 1,
)

evolucao_mensal = (
    fact_pedidos.join(
        spark.table("gold.dim_data"), F.col("order_date_key") == F.col("date_key"), "left"
    )
    .groupBy("year", "month")
    .agg(F.sum("net_amount").alias("receita"))
)
check(
    "evolução temporal mensal é calculável via dim_data (>= 12 meses no período do case)",
    evolucao_mensal.count() >= 12,
)

gargalo_transportadora = (
    spark.table("gold.fact_entregas")
    .filter(F.col("lead_time_dias").isNotNull())
    .groupBy("carrier_name", "carrier_mode")
    .agg(F.avg("lead_time_dias").alias("lead_time_medio"))
)
check(
    "gargalo operacional (lead time por transportadora/modal) é calculável",
    gargalo_transportadora.count() > 0,
)

# ---------------------------------------------------------------------------
if failures:
    print(f"\n{len(failures)} falha(s):")
    for f in failures:
        print(f" - {f}")
    sys.exit(1)
else:
    print("\nTodos os testes passaram.")
