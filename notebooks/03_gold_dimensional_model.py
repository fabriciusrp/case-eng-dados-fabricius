# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: Modelo dimensional para consumo por BI
# MAGIC
# MAGIC Constrói o esquema estrela a partir das 9 tabelas `silver.*`: dimensões (entidades
# MAGIC principais do negócio) separadas de fatos (eventos/transações), em grãos explícitos.
# MAGIC Ver `docs/02_arquitetura_e_modelagem.md`, seção 4, para a justificativa de cada decisão
# MAGIC de granularidade e resolução de FK órfã.

# COMMAND ----------

import os
import sys

try:
    # Databricks: o notebook não tem `__file__`; deriva o próprio diretório a partir do
    # caminho do notebook no workspace (também montado localmente em /Workspace/...).
    _notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
    sys.path.append("/Workspace" + os.path.dirname(_notebook_path))
except NameError:
    # Execução local (fora do Databricks): usa o diretório do próprio arquivo .py
    sys.path.append(os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else ".")
from utils import DQ_NAO_INFORMADO, get_or_create_spark, synthetic_row  # noqa: E402

from pyspark.sql import functions as F

spark = get_or_create_spark()
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


def write_gold(df, table_name: str):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"gold.{table_name}")
    )
    print(f"gold.{table_name}: {df.count()} linhas")


CLIENTE_DESCONHECIDO = "CLIENTE_DESCONHECIDO"
PRODUTO_DESCONHECIDO = "PRODUTO_DESCONHECIDO"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensões

# COMMAND ----------

# --- dim_cliente ---------------------------------------------------------
silver_clientes = spark.table("silver.clientes")

linha_cliente_desconhecido = synthetic_row(
    silver_clientes,
    customer_id=CLIENTE_DESCONHECIDO,
    nome_cliente="Cliente Desconhecido",
    segmento=DQ_NAO_INFORMADO,
    porte=DQ_NAO_INFORMADO,
    cidade=DQ_NAO_INFORMADO,
    estado_uf=DQ_NAO_INFORMADO,
    status_cliente=DQ_NAO_INFORMADO,
)

dim_cliente = silver_clientes.unionByName(linha_cliente_desconhecido).select(
    "customer_id", "nome_cliente", "segmento", "porte", "cidade", "estado_uf",
    "data_cadastro", "email", "status_cliente",
)
write_gold(dim_cliente, "dim_cliente")

# COMMAND ----------

# --- dim_produto -----------------------------------------------------------
silver_produtos = spark.table("silver.produtos")

linha_produto_desconhecido = synthetic_row(
    silver_produtos,
    product_id=PRODUTO_DESCONHECIDO,
    name="Produto Desconhecido",
    category=DQ_NAO_INFORMADO,
    subcategory=DQ_NAO_INFORMADO,
    status=DQ_NAO_INFORMADO,
    currency=DQ_NAO_INFORMADO,
    family=DQ_NAO_INFORMADO,
)

dim_produto = silver_produtos.unionByName(linha_produto_desconhecido).select(
    "product_id", "name", "category", "subcategory", "status", "list_price", "currency", "family",
)
write_gold(dim_produto, "dim_produto")

# COMMAND ----------

# --- dim_canal ---------------------------------------------------------
dim_canal = spark.table("silver.canais").select("id_canal", "nome_canal", "tipo_canal", "ativo")
write_gold(dim_canal, "dim_canal")

# COMMAND ----------

# --- dim_regiao ---------------------------------------------------------
dim_regiao = spark.table("silver.regioes").select(
    "regional_code", "regional_name", "state_uf", "manager_name", "active_flag"
)
write_gold(dim_regiao, "dim_regiao")

# COMMAND ----------

# --- dim_vendedor (denormalizada: já resolve canal e região) ---------------
silver_vendedores = spark.table("silver.vendedores")
silver_canais = spark.table("silver.canais")
silver_regioes = spark.table("silver.regioes")

dim_vendedor = (
    silver_vendedores.alias("v")
    .join(silver_canais.alias("c"), F.col("v.canal_id") == F.col("c.id_canal"), "left")
    .join(silver_regioes.alias("r"), F.col("v.regional_code") == F.col("r.regional_code"), "left")
    .select(
        F.col("v.seller_id").alias("seller_id"),
        F.col("v.seller_name").alias("seller_name"),
        F.col("v.status").alias("status"),
        F.col("v.hire_date").alias("hire_date"),
        F.col("v.canal_id").alias("canal_id"),
        F.coalesce(F.col("c.nome_canal"), F.lit(DQ_NAO_INFORMADO)).alias("canal_nome"),
        F.coalesce(F.col("c.tipo_canal"), F.lit(DQ_NAO_INFORMADO)).alias("canal_tipo"),
        F.col("v.regional_code").alias("regional_code"),
        F.coalesce(F.col("r.regional_name"), F.lit(DQ_NAO_INFORMADO)).alias("regional_name"),
        F.coalesce(F.col("r.state_uf"), F.lit(DQ_NAO_INFORMADO)).alias("regional_state_uf"),
    )
)
write_gold(dim_vendedor, "dim_vendedor")

# COMMAND ----------

# --- dim_data (calendário) ---------------------------------------------
date_cols_sources = [
    ("silver.pedidos_cabecalho", "order_date"),
    ("silver.pedidos_cabecalho", "promised_date"),
    ("silver.entregas", "shipped_at"),
    ("silver.entregas", "delivered_at"),
    ("silver.ocorrencias", "created_at"),
]
bounds = []
for table, col in date_cols_sources:
    row = spark.table(table).select(
        F.min(F.to_date(col)).alias("mn"), F.max(F.to_date(col)).alias("mx")
    ).first()
    if row["mn"] is not None:
        bounds.append(row["mn"])
        bounds.append(row["mx"])

min_date, max_date = min(bounds), max(bounds)
print(f"dim_data cobre {min_date} a {max_date}")

dim_data = (
    spark.sql(
        f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)) AS date"
    )
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
    .select(
        "date_key", "date", "year", "quarter", "month", "month_name",
        "week_of_year", "day", "day_of_week", "day_name", "is_weekend",
    )
)
write_gold(dim_data, "dim_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fatos

# COMMAND ----------

# --- fact_pedidos (grão: 1 linha por pedido) -------------------------------
silver_pedidos_cab = spark.table("silver.pedidos_cabecalho")
silver_entregas = spark.table("silver.entregas")
clientes_validos = {r["customer_id"] for r in dim_cliente.select("customer_id").collect()}

# uma entrega por pedido (join de grão 1:1 — em caso de múltiplas, mantém a mais recente)
entregas_por_pedido = (
    silver_entregas.groupBy("order_ref")
    .agg(
        F.max("delivered_at").alias("delivered_at"),
        F.max("shipped_at").alias("shipped_at"),
    )
)

fact_pedidos = (
    silver_pedidos_cab.alias("p")
    .join(entregas_por_pedido.alias("e"), F.col("p.order_id") == F.col("e.order_ref"), "left")
    .select(
        F.col("p.order_id").alias("order_id"),
        F.when(
            F.col("p.customer_code").isin(list(clientes_validos)), F.col("p.customer_code")
        ).otherwise(F.lit(CLIENTE_DESCONHECIDO)).alias("customer_id"),
        F.col("p.seller_id").alias("seller_id"),
        F.date_format(F.col("p.order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
        F.col("p.order_date").alias("order_date"),
        F.col("p.promised_date").alias("promised_date"),
        F.col("p.status_order").alias("status_order"),
        F.col("p.priority").alias("priority"),
        F.col("p.source").alias("source"),
        F.col("p.gross_amount").alias("gross_amount"),
        F.col("p.discount_amount").alias("discount_amount"),
        F.col("p.net_amount").alias("net_amount"),
        (F.col("p.status_order") == "cancelado").alias("is_cancelado"),
        F.col("e.delivered_at").alias("delivered_at"),
        F.when(
            F.col("e.delivered_at").isNotNull() & F.col("p.promised_date").isNotNull(),
            F.to_date(F.col("e.delivered_at")) > F.col("p.promised_date"),
        ).alias("is_atrasado"),
    )
)
write_gold(fact_pedidos, "fact_pedidos")

# COMMAND ----------

# --- fact_pedidos_itens (grão: 1 linha por item de pedido) -----------------
silver_pedidos_itens = spark.table("silver.pedidos_itens")
produtos_validos = {r["product_id"] for r in dim_produto.select("product_id").collect()}

fact_pedidos_itens = (
    silver_pedidos_itens.alias("i")
    .join(silver_pedidos_cab.alias("p"), F.col("i.order_id") == F.col("p.order_id"), "inner")
    .select(
        F.col("i.order_id").alias("order_id"),
        F.col("i.item_seq").alias("item_seq"),
        F.when(
            F.col("i.product_code").isin(list(produtos_validos)), F.col("i.product_code")
        ).otherwise(F.lit(PRODUTO_DESCONHECIDO)).alias("product_id"),
        F.when(
            F.col("p.customer_code").isin(list(clientes_validos)), F.col("p.customer_code")
        ).otherwise(F.lit(CLIENTE_DESCONHECIDO)).alias("customer_id"),
        F.col("p.seller_id").alias("seller_id"),
        F.date_format(F.col("p.order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
        F.col("i.quantity").alias("quantity"),
        F.col("i.unit_price").alias("unit_price"),
        F.col("i.total_item").alias("total_item"),
        F.col("i.item_status").alias("item_status"),
    )
)
write_gold(fact_pedidos_itens, "fact_pedidos_itens")

# COMMAND ----------

# --- fact_entregas (grão: 1 linha por entrega) ------------------------------
fact_entregas = silver_entregas.select(
    "delivery_id",
    "order_ref",
    "carrier_name",
    "carrier_mode",
    "delivery_status",
    "shipped_at",
    "delivered_at",
    F.date_format("shipped_at", "yyyyMMdd").cast("int").alias("shipped_date_key"),
    "destination_state_uf",
    "destination_city",
    "cost",
).withColumn(
    "lead_time_dias",
    F.when(
        F.col("shipped_at").isNotNull() & F.col("delivered_at").isNotNull(),
        F.datediff(F.col("delivered_at"), F.col("shipped_at")),
    ),
)
write_gold(fact_entregas, "fact_entregas")

# COMMAND ----------

# --- fact_ocorrencias (grão: 1 linha por ticket) ----------------------------
silver_ocorrencias = spark.table("silver.ocorrencias")

fact_ocorrencias = silver_ocorrencias.select(
    "ticket_id",
    "order_id",
    F.when(
        F.col("customer_code").isin(list(clientes_validos)), F.col("customer_code")
    ).otherwise(F.lit(CLIENTE_DESCONHECIDO)).alias("customer_id"),
    "event_type",
    "severity",
    "status",
    "created_at",
    F.date_format("created_at", "yyyyMMdd").cast("int").alias("created_date_key"),
    (F.col("status") == "open").alias("is_aberto"),
)
write_gold(fact_ocorrencias, "fact_ocorrencias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da camada Gold

# COMMAND ----------

for t in [
    "dim_cliente", "dim_produto", "dim_canal", "dim_regiao", "dim_vendedor", "dim_data",
    "fact_pedidos", "fact_pedidos_itens", "fact_entregas", "fact_ocorrencias",
]:
    n = spark.table(f"gold.{t}").count()
    print(f"gold.{t:20s} {n:6d} linhas")
