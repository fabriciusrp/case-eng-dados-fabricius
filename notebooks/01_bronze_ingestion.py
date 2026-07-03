# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Ingestão das fontes brutas
# MAGIC
# MAGIC Ingestão 1:1 das 9 fontes em `sources/`. Sem lógica de negócio, sem limpeza — apenas:
# MAGIC - leitura no formato nativo de cada fonte (CSV `;`/`,`, pipe-delimited, JSON aninhado,
# MAGIC   NDJSON, Excel);
# MAGIC - adição de colunas técnicas `_ingested_at` e `_source_file`;
# MAGIC - gravação como tabela Delta em `bronze.<nome>`.
# MAGIC
# MAGIC Ver decisões e alternativas em `docs/02_arquitetura_e_modelagem.md`, seção 2.

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
from utils import get_or_create_spark, get_base_path  # noqa: E402

spark = get_or_create_spark()
BASE_PATH = get_base_path()
# Padrão aponta para Workspace Files (/Workspace/...), não DBFS root: muitos workspaces
# recentes (incluindo o usado para validar esta entrega) vêm com o DBFS público desabilitado
# por padrão — Workspace Files é o caminho recomendado atualmente pela própria Databricks.
SOURCE_PATH = os.environ.get("CASE_DE_SOURCE_PATH", "file:/Workspace/Shared/case_de/sources")


def to_local_path(path: str) -> str:
    """Converte uma URI `file:...` (usada pelo Spark) no caminho de sistema de arquivos
    equivalente (usado por `pandas.read_excel`). Trata tanto `file:///C:/...` (Windows local)
    quanto `file:/Workspace/...` (Databricks Workspace Files)."""
    for prefix in ("file:///", "file://", "file:"):
        if path.startswith(prefix):
            return path[len(prefix):]
    return path


print(f"BASE_PATH  = {BASE_PATH}")
print(f"SOURCE_PATH = {SOURCE_PATH}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze")

# COMMAND ----------

from pyspark.sql import functions as F


def write_bronze(df, table_name: str, source_file: str):
    """Adiciona metadados técnicos e grava a tabela Delta na camada Bronze."""
    out = df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.lit(source_file)
    )
    (
        out.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"bronze.{table_name}")
    )
    print(f"bronze.{table_name}: {out.count()} linhas")
    return out


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. cadastro_produtos_api_dump.json — produtos (JSON aninhado)

# COMMAND ----------

df_produtos = spark.read.option("multiLine", "true").json(
    f"{SOURCE_PATH}/cadastro_produtos_api_dump.json"
)
write_bronze(df_produtos, "produtos", "cadastro_produtos_api_dump.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. erp_pedidos_cabecalho_2025.csv — cabeçalho de pedidos (CSV `;`, JSON aninhado em coluna)

# COMMAND ----------

df_pedidos_cab = (
    spark.read.option("header", "true")
    .option("sep", ";")
    .option("quote", '"')
    .option("escape", '"')
    .csv(f"{SOURCE_PATH}/erp_pedidos_cabecalho_2025.csv")
)
write_bronze(df_pedidos_cab, "pedidos_cabecalho", "erp_pedidos_cabecalho_2025.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. erp_pedidos_itens_2025.csv — itens de pedido (CSV `,`)

# COMMAND ----------

df_pedidos_itens = (
    spark.read.option("header", "true")
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .csv(f"{SOURCE_PATH}/erp_pedidos_itens_2025.csv")
)
write_bronze(df_pedidos_itens, "pedidos_itens", "erp_pedidos_itens_2025.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. legado_regioes_pipe.txt — regiões (pipe-delimited)

# COMMAND ----------

df_regioes = (
    spark.read.option("header", "true").option("sep", "|").csv(f"{SOURCE_PATH}/legado_regioes_pipe.txt")
)
write_bronze(df_regioes, "regioes", "legado_regioes_pipe.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. vendedores.csv — vendedores (CSV `;`)

# COMMAND ----------

df_vendedores = (
    spark.read.option("header", "true").option("sep", ";").csv(f"{SOURCE_PATH}/vendedores.csv")
)
write_bronze(df_vendedores, "vendedores", "vendedores.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. logistica_entregas.json — entregas (JSON aninhado)

# COMMAND ----------

df_entregas = spark.read.option("multiLine", "true").json(
    f"{SOURCE_PATH}/logistica_entregas.json"
)
write_bronze(df_entregas, "entregas", "logistica_entregas.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. atendimento_ocorrencias.ndjson — ocorrências de atendimento (NDJSON)

# COMMAND ----------

df_ocorrencias = spark.read.json(f"{SOURCE_PATH}/atendimento_ocorrencias.ndjson")
write_bronze(df_ocorrencias, "ocorrencias", "atendimento_ocorrencias.ndjson")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. comercial_canais.xlsx — canais comerciais (Excel)
# MAGIC
# MAGIC Sem `spark-excel` disponível no cluster Community por padrão: lido via `pandas` +
# MAGIC `spark.createDataFrame`, decisão documentada em `docs/02_arquitetura_e_modelagem.md`.

# COMMAND ----------

import pandas as pd

pdf_canais = pd.read_excel(f"{to_local_path(SOURCE_PATH)}/comercial_canais.xlsx")
df_canais = spark.createDataFrame(pdf_canais)
write_bronze(df_canais, "canais", "comercial_canais.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. crm_clientes_export.xlsx — clientes (Excel)

# COMMAND ----------

pdf_clientes = pd.read_excel(f"{to_local_path(SOURCE_PATH)}/crm_clientes_export.xlsx")
df_clientes = spark.createDataFrame(pdf_clientes)
write_bronze(df_clientes, "clientes", "crm_clientes_export.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da ingestão

# COMMAND ----------

for t in ["produtos", "pedidos_cabecalho", "pedidos_itens", "regioes", "vendedores", "entregas", "ocorrencias", "canais", "clientes"]:
    n = spark.table(f"bronze.{t}").count()
    print(f"bronze.{t:20s} {n:6d} linhas")
