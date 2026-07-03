"""
Testes de regressão para as regras de qualidade da camada Silver.

Cada assert aqui corresponde a um caso concreto documentado em
`docs/01_data_quality_findings.md` — existem para impedir que uma futura mudança em
`notebooks/utils.py` ou `notebooks/02_silver_transformation.py` quebre silenciosamente uma
decisão de tratamento já validada contra os dados reais do case.

Uso:
    source scripts/env_spark.sh   # configura JAVA_HOME/HADOOP_HOME/etc (Windows local)
    python notebooks/01_bronze_ingestion.py
    python notebooks/02_silver_transformation.py
    python tests/test_silver_quality_rules.py
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "notebooks"))
from utils import get_or_create_spark  # noqa: E402

spark = get_or_create_spark()

failures = []


def check(description, condition):
    status = "OK  " if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


# ---------------------------------------------------------------------------
# Deduplicação por chave de negócio (contagens finais)
# ---------------------------------------------------------------------------

counts = {t: spark.table(f"silver.{t}").count() for t in [
    "produtos", "canais", "regioes", "vendedores", "clientes",
    "pedidos_cabecalho", "pedidos_itens", "entregas", "ocorrencias",
]}

check("silver.produtos tem 71 linhas (72 brutas - 1 duplicata P0006)", counts["produtos"] == 71)
check("silver.canais tem 7 linhas (8 brutas - 1 duplicata CH05)", counts["canais"] == 7)
check("silver.regioes tem 6 linhas (8 brutas - SE duplicado - sul/S colapsam)", counts["regioes"] == 6)
check("silver.vendedores tem 40 linhas (42 brutas - V004 - V008 duplicados)", counts["vendedores"] == 40)
check("silver.clientes tem 180 linhas (183 brutas - C0010/C0025/C0051 duplicados)", counts["clientes"] == 180)
check("silver.pedidos_cabecalho tem 400 linhas (403 brutas - O00011/O00081/O00121)", counts["pedidos_cabecalho"] == 400)
check(
    "silver.pedidos_itens tem 992 linhas (995 brutas - 3 duplicatas por (order_id,item_seq))",
    counts["pedidos_itens"] == 992,
)
check("silver.entregas tem 320 linhas (322 brutas - D00004/D00021 duplicados)", counts["entregas"] == 320)
check("silver.ocorrencias tem 270 linhas (sem duplicatas na fonte)", counts["ocorrencias"] == 270)

# ---------------------------------------------------------------------------
# Regras de desempate específicas (ver docs/01_data_quality_findings.md)
# ---------------------------------------------------------------------------

produto_p0006 = spark.table("silver.produtos").filter("product_id = 'P0006'").collect()
check("P0006 mantém a versão 'revisado' (updated_at mais recente)",
      len(produto_p0006) == 1 and produto_p0006[0]["name"] == "Produto 6 revisado")

vendedor_v004 = spark.table("silver.vendedores").filter("seller_id = 'V004'").collect()
check("V004 mantém canal_id=CH02 (descarta o órfão CH99)",
      len(vendedor_v004) == 1 and vendedor_v004[0]["canal_id"] == "CH02")

vendedor_v008 = spark.table("silver.vendedores").filter("seller_id = 'V008'").collect()
check("V008 mantém nome sem sufixo 'duplicado'",
      len(vendedor_v008) == 1 and "duplicado" not in vendedor_v008[0]["seller_name"].lower())

canal_ch05 = spark.table("silver.canais").filter("id_canal = 'CH05'").collect()
check("CH05 mantém a linha sem marca de 'duplicado conflitante'",
      len(canal_ch05) == 1 and (canal_ch05[0]["observacao"] is None or "duplicado" not in str(canal_ch05[0]["observacao"]).lower()))

cliente_c0051 = spark.table("silver.clientes").filter("customer_id = 'C0051'").collect()
check("C0051 mantém e-mail válido em empate de updated_at",
      len(cliente_c0051) == 1 and cliente_c0051[0]["email"] == "cliente51@empresa.com")

pedido_o00081 = spark.table("silver.pedidos_cabecalho").filter("order_id = 'O00081'").collect()
check("O00081 mantém gross_amount não-nulo em empate de last_update",
      len(pedido_o00081) == 1 and pedido_o00081[0]["gross_amount"] == 305.52)

entrega_d00004 = spark.table("silver.entregas").filter("delivery_id = 'D00004'").collect()
check("D00004 mantém order_ref válido (O00250), descarta o órfão O99999",
      len(entrega_d00004) == 1 and entrega_d00004[0]["order_ref"] == "O00250")

entrega_d00021 = spark.table("silver.entregas").filter("delivery_id = 'D00021'").collect()
check("D00021 mantém shipped_at válido (descarta '31/02/2025', data inexistente)",
      len(entrega_d00021) == 1 and entrega_d00021[0]["shipped_at"] is not None)

item_o00044_seq3 = (
    spark.table("silver.pedidos_itens")
    .filter("order_id = 'O00044' and item_seq = 3")
    .collect()
)
check(
    "O00044/item 3 mantém product_code=P0047 (produto válido), descarta o órfão P8888",
    len(item_o00044_seq3) == 1 and item_o00044_seq3[0]["product_code"] == "P0047",
)

# ---------------------------------------------------------------------------
# Parsing defensivo de datas inválidas (round-trip validation)
# ---------------------------------------------------------------------------

from pyspark.sql import functions as F  # noqa: E402
from utils import parse_multi_format_timestamp  # noqa: E402

df_datas_invalidas = spark.createDataFrame(
    [("2025-13-40",), ("31/02/2025 10:00",), ("2025-02-24",), ("21/01/2026 00:00",)], ["raw"]
).withColumn("parsed", parse_multi_format_timestamp(F.col("raw"))).collect()

check("'2025-13-40' (mês inválido) vira null, não sofre rollover silencioso",
      df_datas_invalidas[0]["parsed"] is None)
check("'31/02/2025 10:00' (dia inválido) vira null",
      df_datas_invalidas[1]["parsed"] is None)
check("'2025-02-24' (válida) é parseada corretamente",
      df_datas_invalidas[2]["parsed"] is not None)
check("'21/01/2026 00:00' (válida, formato BR com hora) é parseada corretamente",
      df_datas_invalidas[3]["parsed"] is not None)

# ---------------------------------------------------------------------------
# Padronização de enums (nenhuma categoria "suja" deve sobreviver)
# ---------------------------------------------------------------------------

status_order_values = {r["status_order"] for r in spark.table("silver.pedidos_cabecalho").select("status_order").distinct().collect()}
check("status_order tem exatamente 5 categorias padronizadas",
      status_order_values == {"faturado", "em_separacao", "entregue", "cancelado", "nao_informado"})

carrier_mode_values = {r["carrier_mode"] for r in spark.table("silver.entregas").select("carrier_mode").distinct().collect()}
check("carrier_mode tem exatamente 3 categorias (sem variantes de acento/casing)",
      carrier_mode_values == {"rodoviario", "aereo", "nao_informado"})

delivery_status_values = {r["delivery_status"] for r in spark.table("silver.entregas").select("delivery_status").distinct().collect()}
check("delivery_status unifica vocabulário PT/EN ('atrasado' -> 'delayed')",
      delivery_status_values == {"in_transit", "delivered", "cancelled", "delayed", "nao_informado"})

# ---------------------------------------------------------------------------
if failures:
    print(f"\n{len(failures)} falha(s):")
    for f in failures:
        print(f" - {f}")
    sys.exit(1)
else:
    print("\nTodos os testes passaram.")
