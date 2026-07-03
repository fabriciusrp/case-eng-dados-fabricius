# Databricks notebook source
# MAGIC %md
# MAGIC # Utilitários compartilhados de tratamento de qualidade
# MAGIC
# MAGIC Funções reutilizadas pelos notebooks de Silver (`02_silver_transformation`) e, em menor
# MAGIC grau, pelo Gold. Cada função implementa um dos "padrões transversais" documentados em
# MAGIC `docs/01_data_quality_findings.md`.
# MAGIC
# MAGIC **Como usar este arquivo:**
# MAGIC - No Databricks (Repos): `from utils import *` funciona diretamente se este arquivo estiver
# MAGIC   no mesmo diretório do notebook que o importa.
# MAGIC - Em notebook clássico (não-Repos): use `%run ./utils` na primeira célula do notebook.
# MAGIC - Localmente (fora do Databricks): `python -c "import utils"` funciona normalmente, é um
# MAGIC   módulo Python padrão — as linhas `# MAGIC` acima são comentários para o Databricks e são
# MAGIC   ignoradas pelo interpretador Python.

import os

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DQ_NAO_INFORMADO = "nao_informado"


def get_or_create_spark(app_name: str = "case-de-pipeline"):
    """Retorna a SparkSession ativa do Databricks (variável global `spark`) quando o
    notebook roda lá, ou cria uma SparkSession local com Delta configurado quando roda
    fora do Databricks (desenvolvimento/teste local). Mantém o mesmo código de notebook
    portável entre os dois ambientes — ver `docs/02_arquitetura_e_modelagem.md`, seção 6."""
    try:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            return active
    except Exception:
        pass

    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    warehouse_dir = os.environ.get("CASE_DE_WAREHOUSE_DIR", "./spark-warehouse")
    metastore_dir = os.environ.get("CASE_DE_METASTORE_DIR", "./metastore_db")

    # .enableHiveSupport() usa um metastore Derby embutido persistido em disco (metastore_dir),
    # necessário para que tabelas registradas via saveAsTable sobrevivam entre processos Python
    # separados (cada notebook local roda como um processo `python notebook.py` independente).
    # No Databricks, o workspace já provê um metastore persistente nativamente — este bloco só
    # existe para viabilizar o teste local, não é executado lá (ver get_or_create_spark acima).
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.ui.showConsoleProgress", "false")
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_dir};create=true",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_base_path() -> str:
    """Caminho raiz onde as camadas Delta são gravadas. No Databricks, aponte para um
    volume/Unity Catalog external location ou DBFS (ex.: `dbfs:/FileStore/case_de`). Em
    execução local, definido via variável de ambiente `CASE_DE_BASE_PATH`."""
    return os.environ.get("CASE_DE_BASE_PATH", "dbfs:/FileStore/case_de")

# ---------------------------------------------------------------------------
# 1. Normalização de chaves de negócio
# ---------------------------------------------------------------------------

def normalize_key(col: Column) -> Column:
    """Upper-case + trim. Usado em toda chave de negócio (order_id, product_code,
    customer_id, seller_id, canal_id, regional_code) antes de qualquer join."""
    return F.upper(F.trim(col))


# ---------------------------------------------------------------------------
# 2. Padronização de enums / status
# ---------------------------------------------------------------------------

def standardize_lower(col: Column, default: str = DQ_NAO_INFORMADO) -> Column:
    """Padroniza para minúsculo + trim; nulo/vazio vira `default`.
    Usado quando o único problema é casing (ex.: item_status: ativo/Ativo)."""
    cleaned = F.lower(F.trim(col))
    return F.when((cleaned.isNull()) | (cleaned == ""), F.lit(default)).otherwise(cleaned)


def standardize_enum(col: Column, mapping: dict, default: str = DQ_NAO_INFORMADO) -> Column:
    """Padroniza para minúsculo + aplica de-para explícito (ex.: 'em separacao' -> 'em_separacao').
    Chaves do `mapping` devem estar em minúsculo. Valores não mapeados E não nulos são mantidos
    como estão (não forçados a `default`), para não esconder categorias novas/inesperadas."""
    cleaned = F.lower(F.trim(col))
    mapping_expr = F.create_map([F.lit(x) for kv in mapping.items() for x in kv])
    return F.when(
        (cleaned.isNull()) | (cleaned == ""), F.lit(default)
    ).otherwise(F.coalesce(mapping_expr[cleaned], cleaned))


STATUS_ORDER_MAP = {
    "faturado": "faturado",
    "em_separacao": "em_separacao",
    "em separacao": "em_separacao",
    "entregue": "entregue",
    "cancelado": "cancelado",
}

DELIVERY_STATUS_MAP = {
    "in_transit": "in_transit",
    "delivered": "delivered",
    "cancelled": "cancelled",
    "atrasado": "delayed",  # unifica vocabulário PT/EN misturado na fonte
}

EVENT_TYPE_MAP = {
    "delay": "delay",
    "refund": "refund",
    "troca": "exchange",  # unifica vocabulário PT/EN misturado na fonte
    "cancel_request": "cancel_request",
    "complaint": "complaint",
}

CARRIER_MODE_MAP = {
    "rodoviário": "rodoviario",
    "rodoviario": "rodoviario",
    "aéreo": "aereo",
    "aereo": "aereo",
}

CHANNEL_TYPE_MAP = {
    "direto": "Direto",
    "indireto": "Indireto",
    "digital": "Digital",
}

CLIENT_SIZE_MAP = {
    "grande": "Grande",
    "média": "Média",
    "media": "Média",
    "pequena": "Pequena",
}


# ---------------------------------------------------------------------------
# 3. Parsing de datas em múltiplos formatos
# ---------------------------------------------------------------------------

_DATETIME_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "dd/MM/yyyy HH:mm",
    "yyyy-MM-dd",
    "dd/MM/yyyy",
    "yyyy/MM/dd",
]


def _parse_with_roundtrip_validation(trimmed: Column, fmt: str) -> Column:
    """Faz o parse com `fmt` e só aceita o resultado se, ao formatar de volta com o mesmo
    padrão, reproduzir exatamente a string original. Necessário porque `to_timestamp` do
    Spark faz *rollover* silencioso de campos fora do intervalo (ex.: mês 13, dia 31 de
    fevereiro) em vez de rejeitar — sem essa validação, '2025-13-40' viraria uma data válida
    (mas errada) em vez de `null`."""
    parsed = F.to_timestamp(trimmed, fmt)
    reformatted = F.date_format(parsed, fmt)
    return F.when(parsed.isNotNull() & (reformatted == trimmed), parsed)


def parse_multi_format_timestamp(col: Column) -> Column:
    """Tenta múltiplos formatos de data/hora observados nas fontes, nessa ordem, via
    coalesce. Datas estruturalmente incompatíveis ou logicamente inválidas (ex.:
    '2025-13-40', '31/02/2025' — mês/dia fora do intervalo) resultam em `null` graças à
    validação de round-trip em `_parse_with_roundtrip_validation`."""
    trimmed = F.trim(col)
    candidates = [_parse_with_roundtrip_validation(trimmed, fmt) for fmt in _DATETIME_FORMATS]
    return F.coalesce(*candidates)


def parse_multi_format_date(col: Column) -> Column:
    """Mesma lógica de `parse_multi_format_timestamp`, mas retorna apenas a parte de data."""
    return F.to_date(parse_multi_format_timestamp(col))


# ---------------------------------------------------------------------------
# 4. Números com separador decimal BR e valores sentinela textuais
# ---------------------------------------------------------------------------

def parse_br_decimal_to_double(col: Column) -> Column:
    """Converte string com vírgula decimal BR ('1274,78') ou ponto ('1274.78') para double.
    Aceita ambos os formatos, pois as fontes misturam os dois."""
    normalized = F.regexp_replace(F.trim(col.cast("string")), ",", ".")
    return normalized.cast("double")


def safe_cast_double(col: Column) -> Column:
    """Cast defensivo para double: valores sentinela textuais ('N/A', 'unknown', '') viram
    null em vez de quebrar o job ou virar NaN silenciosamente."""
    as_string = F.trim(F.lower(col.cast("string")))
    is_sentinel = as_string.isin("n/a", "unknown", "null", "")
    return F.when(is_sentinel | as_string.isNull(), None).otherwise(
        parse_br_decimal_to_double(col)
    )


def add_dq_flag(df: DataFrame, flag_col: str, condition: Column, flag_name: str) -> DataFrame:
    """Adiciona/atualiza uma coluna array `_dq_flags` com `flag_name` quando `condition` é
    verdadeira. Convenção: chamar em sequência para acumular múltiplas flags na mesma linha."""
    if flag_col not in df.columns:
        df = df.withColumn(flag_col, F.array().cast("array<string>"))
    return df.withColumn(
        flag_col,
        F.when(condition, F.array_union(F.col(flag_col), F.array(F.lit(flag_name)))).otherwise(
            F.col(flag_col)
        ),
    )


# ---------------------------------------------------------------------------
# 5. Normalização de estado/UF (vocabulário fechado, várias grafias por fonte)
# ---------------------------------------------------------------------------

STATE_TO_UF = {
    "mg": "MG", "minas gerais": "MG",
    "pr": "PR", "parana": "PR", "paraná": "PR",
    "rj": "RJ", "rio de janeiro": "RJ",
    "sc": "SC", "santa catarina": "SC", "sta catarina": "SC", "s. catarina": "SC",
    "sp": "SP", "sao paulo": "SP", "são paulo": "SP",
    "am": "AM", "amazonas": "AM",
    "ba": "BA", "bahia": "BA",
    "go": "GO", "goias": "GO", "goiás": "GO",
}


def normalize_state_to_uf(col: Column) -> Column:
    """Mapeia as dezenas de grafias de estado observadas (sigla, nome completo, com/sem
    acento, abreviação parcial) para a sigla UF de 2 letras. Valor não mapeado é mantido
    em upper-case (não descartado) para não perder informação geográfica desconhecida."""
    cleaned = F.lower(F.trim(col))
    mapping_expr = F.create_map([F.lit(x) for kv in STATE_TO_UF.items() for x in kv])
    return F.coalesce(mapping_expr[cleaned], F.upper(cleaned))


# ---------------------------------------------------------------------------
# 6. De-para de código regional legado
# ---------------------------------------------------------------------------

REGIONAL_CODE_ALIAS = {
    "SUL": "S",  # 'sul' é o mesmo código regional que 'S', apenas grafia legada alternativa
}


def normalize_regional_code(col: Column) -> Column:
    upper = normalize_key(col)
    mapping_expr = F.create_map([F.lit(x) for kv in REGIONAL_CODE_ALIAS.items() for x in kv])
    return F.coalesce(mapping_expr[upper], upper)


# ---------------------------------------------------------------------------
# 7. Deduplicação "mais recente vence"
# ---------------------------------------------------------------------------

def dedup_keep_latest(df: DataFrame, key_cols: list, order_col: str) -> DataFrame:
    """Remove duplicatas por `key_cols`, mantendo a linha com maior valor de `order_col`
    (tipicamente um timestamp de atualização). Em empate, mantém a primeira ocorrência
    determinística pela ordem original (monotonically_increasing_id)."""
    return dedup_by_rank(df, key_cols, [F.col(order_col).desc_nulls_last()])


def dedup_by_rank(df: DataFrame, key_cols: list, order_by_cols: list) -> DataFrame:
    """Versão genérica de deduplicação: mantém, por `key_cols`, a linha "melhor ranqueada"
    segundo `order_by_cols` (lista de expressões Column já com `.asc()`/`.desc()` aplicado,
    avaliadas em ordem de prioridade). Em empate total, mantém a primeira ocorrência do
    arquivo original. Usada quando o critério de desempate não é um simples timestamp
    (ex.: preferir registro sem sufixo 'duplicado', ou com FK válida) — ver
    `docs/01_data_quality_findings.md` para a justificativa de cada regra concreta.

    Nota: `coalesce(1)` antes de gerar o id de desempate garante que "primeira ocorrência do
    arquivo" seja determinístico (`monotonically_increasing_id()` não preserva ordem global
    de forma confiável quando o DataFrame tem múltiplas partições). Aceitável aqui pelo volume
    pequeno de dados do case; para volumes maiores, prefira sempre um critério de desempate
    baseado em coluna de negócio (timestamp, etc.) em vez de ordem de arquivo."""
    df = df.coalesce(1).withColumn("_tmp_row_id", F.monotonically_increasing_id())
    w = Window.partitionBy(*key_cols).orderBy(*order_by_cols, F.col("_tmp_row_id").asc())
    return (
        df.withColumn("_tmp_rn", F.row_number().over(w))
        .filter(F.col("_tmp_rn") == 1)
        .drop("_tmp_rn", "_tmp_row_id")
    )


def synthetic_row(df: DataFrame, **values) -> DataFrame:
    """Cria um DataFrame de 1 linha com o mesmo schema de `df`, útil para adicionar uma linha
    sintética de dimensão (ex.: 'CLIENTE_DESCONHECIDO') sem risco de erro de tipo em colunas
    complexas (array/struct/timestamp). Colunas não passadas em `values` viram `null` tipado;
    colunas passadas são convertidas (`cast`) para o tipo declarado no schema de `df`."""
    exprs = []
    for field in df.schema.fields:
        if field.name in values:
            exprs.append(F.lit(values[field.name]).cast(field.dataType).alias(field.name))
        else:
            exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.sparkSession.range(1).select(*exprs)


def is_valid_email(col: Column) -> Column:
    """Validação simples de formato de e-mail (usuario@dominio.tld). Não valida existência
    real do endereço, apenas formato — suficiente para sinalizar `dq_invalid_email`."""
    return col.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
