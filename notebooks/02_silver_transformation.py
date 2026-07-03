# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: Limpeza, padronização e deduplicação
# MAGIC
# MAGIC Lê as 9 tabelas `bronze.*` e produz 9 tabelas `silver.*` conformadas: chaves
# MAGIC normalizadas, enums padronizados, datas/números parseados, duplicatas resolvidas e
# MAGIC uma coluna de auditoria `_dq_flags` por linha. Todas as regras aqui implementadas estão
# MAGIC documentadas e justificadas em `docs/01_data_quality_findings.md`.
# MAGIC
# MAGIC Esta camada **não faz modelagem** (isso é trabalho do Gold) — só corrige e conforma.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else ".")
from utils import (  # noqa: E402
    CARRIER_MODE_MAP,
    CHANNEL_TYPE_MAP,
    CLIENT_SIZE_MAP,
    DELIVERY_STATUS_MAP,
    DQ_NAO_INFORMADO,
    EVENT_TYPE_MAP,
    STATUS_ORDER_MAP,
    add_dq_flag,
    dedup_by_rank,
    dedup_keep_latest,
    get_or_create_spark,
    is_valid_email,
    normalize_key,
    normalize_regional_code,
    normalize_state_to_uf,
    parse_br_decimal_to_double,
    parse_multi_format_date,
    parse_multi_format_timestamp,
    safe_cast_double,
    standardize_enum,
    standardize_lower,
)

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = get_or_create_spark()
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")


def write_silver(df, table_name: str):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"silver.{table_name}")
    )
    print(f"silver.{table_name}: {df.count()} linhas")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. silver.produtos

# COMMAND ----------

bronze_produtos = spark.table("bronze.produtos")

silver_produtos = (
    bronze_produtos.select(
        normalize_key(F.col("product.product_id")).alias("product_id"),
        F.col("product.name").alias("name"),
        F.col("product.category").alias("category"),
        F.coalesce(F.trim(F.col("product.subcategory")), F.lit(DQ_NAO_INFORMADO)).alias("subcategory"),
        standardize_lower(F.col("product.status")).alias("status"),
        F.col("pricing.list_price").alias("list_price_raw"),
        F.col("pricing.currency").alias("currency"),
        F.coalesce(F.trim(F.col("attributes.family")), F.lit(DQ_NAO_INFORMADO)).alias("family"),
        F.col("attributes.tags").alias("tags"),
        parse_multi_format_timestamp(F.col("updated_at")).alias("updated_at"),
    )
    .withColumn("list_price", safe_cast_double(F.col("list_price_raw")))
    .drop("list_price_raw")
)

silver_produtos = add_dq_flag(
    silver_produtos, "_dq_flags", F.col("name").isNull(), "missing_name"
)
silver_produtos = add_dq_flag(
    silver_produtos,
    "_dq_flags",
    F.col("list_price").isNull(),
    "invalid_or_missing_list_price",
)

silver_produtos = dedup_keep_latest(silver_produtos, ["product_id"], "updated_at")
write_silver(silver_produtos, "produtos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. silver.canais
# MAGIC (construído antes de `silver.vendedores` pois é usado para validar a FK `canal_id`)

# COMMAND ----------

bronze_canais = spark.table("bronze.canais")

silver_canais = bronze_canais.select(
    normalize_key(F.col("id_canal")).alias("id_canal"),
    F.coalesce(F.trim(F.col("nome_canal")), F.lit(DQ_NAO_INFORMADO)).alias("nome_canal"),
    standardize_enum(F.col("tipo_canal"), CHANNEL_TYPE_MAP, default=DQ_NAO_INFORMADO).alias(
        "tipo_canal"
    ),
    F.when(F.lower(F.trim(F.col("ativo"))) == "sim", True)
    .when(F.lower(F.trim(F.col("ativo"))) == "nao", False)
    .otherwise(None)
    .alias("ativo"),
    F.col("observacao").alias("observacao"),
)

# CH05 tem duplicata marcada como "duplicado conflitante" — preferir a linha SEM essa marca
silver_canais = dedup_by_rank(
    silver_canais,
    ["id_canal"],
    [F.lower(F.coalesce(F.col("observacao"), F.lit(""))).contains("duplicado").asc()],
)
write_silver(silver_canais, "canais")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. silver.regioes

# COMMAND ----------

bronze_regioes = spark.table("bronze.regioes")

silver_regioes = bronze_regioes.select(
    normalize_regional_code(F.col("regional_code")).alias("regional_code"),
    F.col("regional_name").alias("regional_name"),
    normalize_state_to_uf(F.col("state")).alias("state_uf"),
    F.col("manager_name").alias("manager_name"),
    (F.col("active_flag") == "1").alias("active_flag"),
)

# Duas linhas de 'SE' colapsam para a mesma chave após normalize_state_to_uf (equivalentes
# em todas as colunas, tanto faz qual sobrevive). 'sul' colapsa com 'S' e as duas têm
# regional_name diferente ("Sul" vs "Região Sul") — prefere a forma curta, consistente com o
# padrão dos demais códigos (Norte/Nordeste/Sudeste/Centro-Oeste).
prefere_nome_curto = ~F.lower(F.col("regional_name")).startswith("região")
silver_regioes = dedup_by_rank(silver_regioes, ["regional_code"], [prefere_nome_curto.desc()])
write_silver(silver_regioes, "regioes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. silver.vendedores

# COMMAND ----------

bronze_vendedores = spark.table("bronze.vendedores")
canais_validos = {r["id_canal"] for r in silver_canais.select("id_canal").collect()}

silver_vendedores = bronze_vendedores.select(
    normalize_key(F.col("seller_id")).alias("seller_id"),
    F.col("seller_name").alias("seller_name"),
    F.coalesce(normalize_key(F.col("canal_id")), F.lit(DQ_NAO_INFORMADO)).alias("canal_id"),
    F.coalesce(normalize_regional_code(F.col("regional_code")), F.lit(DQ_NAO_INFORMADO)).alias(
        "regional_code"
    ),
    parse_multi_format_date(F.col("hire_date")).alias("hire_date"),
    standardize_lower(F.col("status")).alias("status"),
)

canal_valido_expr = F.col("canal_id").isin(list(canais_validos))
duplicado_no_nome = F.lower(F.col("seller_name")).contains("duplicado")

# V004: mantém a linha com canal_id válido (descarta a que aponta pro canal órfão CH99)
# V008: mantém a linha cujo nome não tem o sufixo "duplicado"
silver_vendedores = dedup_by_rank(
    silver_vendedores,
    ["seller_id"],
    [duplicado_no_nome.asc(), canal_valido_expr.desc()],
)
write_silver(silver_vendedores, "vendedores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. silver.clientes

# COMMAND ----------

bronze_clientes = spark.table("bronze.clientes")

silver_clientes = bronze_clientes.select(
    normalize_key(F.col("customer_id")).alias("customer_id"),
    F.col("nome_cliente").alias("nome_cliente"),
    F.coalesce(F.trim(F.col("segmento")), F.lit(DQ_NAO_INFORMADO)).alias("segmento"),
    standardize_enum(F.col("porte"), CLIENT_SIZE_MAP, default=DQ_NAO_INFORMADO).alias("porte"),
    F.col("cidade").alias("cidade"),
    normalize_state_to_uf(F.col("estado")).alias("estado_uf"),
    parse_multi_format_date(F.col("data_cadastro")).alias("data_cadastro"),
    F.col("email").alias("email"),
    standardize_lower(F.col("status_cliente")).alias("status_cliente"),
    parse_multi_format_timestamp(F.col("updated_at")).alias("updated_at"),
)

silver_clientes = add_dq_flag(
    silver_clientes,
    "_dq_flags",
    F.col("email").isNotNull() & ~is_valid_email(F.col("email")),
    "invalid_email_format",
)

# Regra padrão "mais recente vence" (resolve C0010, C0025); em empate de updated_at (caso
# C0051, mesma timestamp nas duas linhas) prefere a linha com e-mail em formato válido —
# critério de conteúdo, não a ordem do arquivo (não confiável após round-trip por Delta).
silver_clientes = dedup_by_rank(
    silver_clientes,
    ["customer_id"],
    [F.col("updated_at").desc_nulls_last(), is_valid_email(F.col("email")).desc()],
)
write_silver(silver_clientes, "clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. silver.pedidos_cabecalho

# COMMAND ----------

bronze_pedidos_cab = spark.table("bronze.pedidos_cabecalho")

payment_schema = StructType(
    [StructField("priority", StringType()), StructField("source", StringType())]
)

silver_pedidos_cab = bronze_pedidos_cab.select(
    normalize_key(F.col("order_id")).alias("order_id"),
    normalize_key(F.col("customer_code")).alias("customer_code"),
    normalize_key(F.col("seller_id")).alias("seller_id"),
    F.col("order_date").alias("order_date_raw"),
    F.col("promised_date").alias("promised_date_raw"),
    standardize_enum(F.col("status_order"), STATUS_ORDER_MAP).alias("status_order"),
    parse_br_decimal_to_double(F.col("gross_amount")).alias("gross_amount"),
    parse_br_decimal_to_double(F.col("discount_amount")).alias("discount_amount"),
    parse_br_decimal_to_double(F.col("net_amount")).alias("net_amount"),
    F.from_json(F.col("payment_details"), payment_schema).alias("payment_details"),
    F.col("last_update").alias("last_update_raw"),
)

silver_pedidos_cab = (
    silver_pedidos_cab.withColumn("order_date", parse_multi_format_date(F.col("order_date_raw")))
    .withColumn("promised_date", parse_multi_format_date(F.col("promised_date_raw")))
    .withColumn("last_update", parse_multi_format_timestamp(F.col("last_update_raw")))
    .withColumn("priority", F.coalesce(F.col("payment_details.priority"), F.lit(DQ_NAO_INFORMADO)))
    .withColumn("source", F.coalesce(F.col("payment_details.source"), F.lit(DQ_NAO_INFORMADO)))
    .withColumn("net_amount_calculado", F.col("gross_amount") - F.col("discount_amount"))
    .drop("order_date_raw", "promised_date_raw", "last_update_raw", "payment_details")
)

silver_pedidos_cab = add_dq_flag(
    silver_pedidos_cab,
    "_dq_flags",
    F.col("order_date").isNull(),
    "invalid_or_missing_order_date",
)
silver_pedidos_cab = add_dq_flag(
    silver_pedidos_cab,
    "_dq_flags",
    F.abs(F.col("net_amount") - F.col("net_amount_calculado")) > F.lit(0.01),
    "net_amount_mismatch",
)
silver_pedidos_cab = add_dq_flag(
    silver_pedidos_cab,
    "_dq_flags",
    F.col("customer_code") == "C9999",
    "placeholder_customer",
)

# Regra padrão "mais recente vence" (resolve O00011); em empate de last_update (casos
# O00081 e O00121, mesma timestamp nas duas linhas) prefere a linha com valores válidos
# (gross_amount e order_date não-nulos) — critério de conteúdo, não ordem de arquivo.
silver_pedidos_cab = dedup_by_rank(
    silver_pedidos_cab,
    ["order_id"],
    [
        F.col("last_update").desc_nulls_last(),
        F.col("gross_amount").isNotNull().desc(),
        F.col("order_date").isNotNull().desc(),
    ],
)
write_silver(silver_pedidos_cab, "pedidos_cabecalho")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. silver.pedidos_itens

# COMMAND ----------

bronze_pedidos_itens = spark.table("bronze.pedidos_itens")

silver_pedidos_itens = bronze_pedidos_itens.select(
    normalize_key(F.col("order_id")).alias("order_id"),
    F.col("item_seq").cast("int").alias("item_seq"),
    normalize_key(F.col("product_code")).alias("product_code"),
    safe_cast_double(F.col("quantity")).alias("quantity"),
    parse_br_decimal_to_double(F.col("unit_price")).alias("unit_price"),
    safe_cast_double(F.col("total_item")).alias("total_item"),
    standardize_lower(F.col("item_status")).alias("item_status"),
)

silver_pedidos_itens = (
    silver_pedidos_itens.withColumn(
        "total_item_calculado", F.col("quantity") * F.col("unit_price")
    )
)

silver_pedidos_itens = add_dq_flag(
    silver_pedidos_itens, "_dq_flags", F.col("quantity").isNull(), "missing_quantity"
)
silver_pedidos_itens = add_dq_flag(
    silver_pedidos_itens, "_dq_flags", F.col("quantity") < 0, "negative_quantity"
)
silver_pedidos_itens = add_dq_flag(
    silver_pedidos_itens, "_dq_flags", F.col("quantity") == 0, "zero_quantity"
)
silver_pedidos_itens = add_dq_flag(
    silver_pedidos_itens,
    "_dq_flags",
    F.abs(F.col("total_item") - F.col("total_item_calculado")) > F.lit(0.01),
    "total_item_mismatch",
)

# Grão real é (order_id, item_seq) — não (order_id, item_seq, product_code). Achado: O00044
# item_seq=3 tem duas linhas com MESMA quantidade/preço/total mas product_code diferente
# (P0047 válido vs P8888 órfão) — é a mesma linha com o código do produto corrompido, não
# dois itens distintos. Desempate: prefere product_code que existe no catálogo, depois
# quantity não-nula (resolve também o caso o00062/P0006 duplicado).
produtos_validos_para_itens = {r["product_id"] for r in silver_produtos.select("product_id").collect()}
produto_valido_expr = F.col("product_code").isin(list(produtos_validos_para_itens))

silver_pedidos_itens = dedup_by_rank(
    silver_pedidos_itens,
    ["order_id", "item_seq"],
    [produto_valido_expr.desc(), F.col("quantity").desc_nulls_last()],
)
write_silver(silver_pedidos_itens, "pedidos_itens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. silver.entregas

# COMMAND ----------

bronze_entregas = spark.table("bronze.entregas")
pedidos_validos = {
    r["order_id"] for r in silver_pedidos_cab.select("order_id").collect()
}

silver_entregas = bronze_entregas.select(
    F.col("delivery_id").alias("delivery_id"),
    normalize_key(F.col("order_ref")).alias("order_ref"),
    F.col("carrier.name").alias("carrier_name"),
    standardize_enum(F.col("carrier.mode"), CARRIER_MODE_MAP).alias("carrier_mode"),
    standardize_enum(F.col("delivery_status"), DELIVERY_STATUS_MAP).alias("delivery_status"),
    F.col("timestamps.shipped_at").alias("shipped_at_raw"),
    F.col("timestamps.delivered_at").alias("delivered_at_raw"),
    normalize_state_to_uf(F.col("destination.state")).alias("destination_state_uf"),
    F.col("destination.city").alias("destination_city"),
    safe_cast_double(F.col("cost")).alias("cost"),
)

silver_entregas = silver_entregas.withColumn(
    "shipped_at", parse_multi_format_timestamp(F.col("shipped_at_raw"))
).withColumn("delivered_at", parse_multi_format_timestamp(F.col("delivered_at_raw"))).drop(
    "shipped_at_raw", "delivered_at_raw"
)

silver_entregas = add_dq_flag(
    silver_entregas, "_dq_flags", F.col("shipped_at").isNull(), "invalid_or_missing_shipped_at"
)
silver_entregas = add_dq_flag(
    silver_entregas,
    "_dq_flags",
    ~F.col("order_ref").isin(list(pedidos_validos)),
    "orphan_order_ref",
)

# D00004 e D00021 duplicados: prefere a linha com shipped_at válido E order_ref não-órfão
order_valido_expr = F.col("order_ref").isin(list(pedidos_validos))
silver_entregas = dedup_by_rank(
    silver_entregas,
    ["delivery_id"],
    [F.col("shipped_at").isNotNull().desc(), order_valido_expr.desc()],
)
write_silver(silver_entregas, "entregas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. silver.ocorrencias

# COMMAND ----------

bronze_ocorrencias = spark.table("bronze.ocorrencias")

silver_ocorrencias = bronze_ocorrencias.select(
    F.col("ticket_id").alias("ticket_id"),
    normalize_key(F.col("order_id")).alias("order_id"),
    F.coalesce(normalize_key(F.col("customer_code")), F.lit(DQ_NAO_INFORMADO)).alias(
        "customer_code"
    ),
    standardize_enum(F.col("event_type"), EVENT_TYPE_MAP).alias("event_type"),
    standardize_lower(F.col("severity")).alias("severity"),
    standardize_lower(F.col("status")).alias("status"),
    F.col("created_at").alias("created_at_raw"),
    F.col("metadata").alias("metadata"),
)

silver_ocorrencias = silver_ocorrencias.withColumn(
    "created_at", parse_multi_format_timestamp(F.col("created_at_raw"))
).withColumn(
    "_time_missing_in_source", ~F.col("created_at_raw").contains(":")
).drop("created_at_raw")

silver_ocorrencias = add_dq_flag(
    silver_ocorrencias,
    "_dq_flags",
    F.col("_time_missing_in_source"),
    "time_missing_assumed_midnight",
).drop("_time_missing_in_source")

write_silver(silver_ocorrencias, "ocorrencias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da camada Silver

# COMMAND ----------

for t in [
    "produtos", "canais", "regioes", "vendedores", "clientes",
    "pedidos_cabecalho", "pedidos_itens", "entregas", "ocorrencias",
]:
    n = spark.table(f"silver.{t}").count()
    print(f"silver.{t:20s} {n:6d} linhas")
