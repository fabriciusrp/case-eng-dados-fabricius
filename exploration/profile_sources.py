"""
Exploração inicial das 9 fontes brutas com pandas (sem Spark) — usado para levantar os
achados documentados em `docs/01_data_quality_findings.md` antes de qualquer transformação.
Mantido no repositório para rastreabilidade do processo de investigação.

Uso: python exploration/profile_sources.py  (a partir da raiz do projeto)
"""

import json
import os

import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 160)

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sources")

def h(title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)

# ---------------------------------------------------------------
h("1. cadastro_produtos_api_dump.json")
products = json.load(open(f"{SRC}/cadastro_produtos_api_dump.json", encoding="utf-8"))
prod_rows = []
for p in products:
    prod = p["product"]
    pricing = p["pricing"]
    attrs = p["attributes"]
    prod_rows.append({
        "product_id": prod["product_id"],
        "name": prod["name"],
        "category": prod["category"],
        "subcategory": prod["subcategory"],
        "status": prod["status"],
        "list_price": pricing["list_price"],
        "currency": pricing["currency"],
        "family": attrs["family"],
        "tags": attrs["tags"],
        "updated_at": p["updated_at"],
    })
dfp = pd.DataFrame(prod_rows)
print("rows:", len(dfp))
print("product_id case variants (upper vs raw):", (dfp["product_id"].str.upper() != dfp["product_id"]).sum())
print("duplicate product_id (case-insensitive):")
dupe_ids = dfp["product_id"].str.upper()
print(dfp[dupe_ids.duplicated(keep=False)].sort_values("product_id")[["product_id","name","updated_at"]])
print("\nstatus value counts:\n", dfp["status"].value_counts(dropna=False))
print("\nlist_price dtype issues (non-numeric strings):")
def is_bad_price(v):
    try:
        float(v)
        return False
    except Exception:
        return True
print(dfp[dfp["list_price"].apply(is_bad_price)][["product_id","list_price"]])
print("\nnull name:", dfp["name"].isna().sum())
print("null subcategory:", dfp["subcategory"].isna().sum())
print("null family:", dfp["family"].isna().sum())
print("\ncategory values:", dfp["category"].unique())

# ---------------------------------------------------------------
h("2. erp_pedidos_cabecalho_2025.csv")
orders = pd.read_csv(f"{SRC}/erp_pedidos_cabecalho_2025.csv", sep=";", dtype=str)
print("rows:", len(orders))
print("columns:", list(orders.columns))
print("\norder_id duplicates (case-insensitive):")
oid_upper = orders["order_id"].str.upper()
print(orders[oid_upper.duplicated(keep=False)].sort_values("order_id")[["order_id","customer_code","status_order","gross_amount","last_update"]])
print("\nstatus_order distinct values:\n", orders["status_order"].value_counts(dropna=False))
print("\norder_date sample formats:", orders["order_date"].dropna().sample(10, random_state=1).tolist())
print("\npromised_date sample formats:", orders["promised_date"].dropna().sample(10, random_state=1).tolist())
print("\ngross_amount non-numeric (comma decimal or text):")
def is_bad_amount(v):
    if pd.isna(v):
        return False
    try:
        float(v)
        return False
    except Exception:
        return True
print(orders[orders["gross_amount"].apply(is_bad_amount)][["order_id","gross_amount"]])
print("\ncustomer_code case variants sample:", orders[orders["customer_code"].str.upper() != orders["customer_code"]]["customer_code"].unique()[:10])
print("\nplaceholder customer C9999 count:", (orders["customer_code"] == "C9999").sum())
print("\ninvalid date 2025-13-40 present:", orders["order_date"].str.contains("2025-13-40", na=False).sum())
print("\nnull status_order count:", orders["status_order"].isna().sum())

# ---------------------------------------------------------------
h("3. erp_pedidos_itens_2025.csv")
items = pd.read_csv(f"{SRC}/erp_pedidos_itens_2025.csv", dtype=str)
print("rows:", len(items))
print("columns:", list(items.columns))
print("\nexact duplicate rows (order_id+item_seq+product_code, case-insensitive on order_id):")
items["order_id_up"] = items["order_id"].str.upper()
dupe_key = items.duplicated(subset=["order_id_up","item_seq","product_code"], keep=False)
print(items[dupe_key].sort_values(["order_id_up","item_seq"]))
print("\nquantity non-numeric or blank:")
print(items[items["quantity"].isna() | (items["quantity"].astype(str).str.strip()=="")][["order_id","item_seq","product_code","quantity"]])
print("\nnegative quantity rows:", (pd.to_numeric(items["quantity"], errors="coerce") < 0).sum())
print("\nzero quantity rows:", (pd.to_numeric(items["quantity"], errors="coerce") == 0).sum())
print("\nunit_price with comma decimals (sample):")
comma_price = items["unit_price"].astype(str).str.contains(",", na=False)
print(items[comma_price][["order_id","item_seq","unit_price"]].head(10))
print("comma-decimal price count:", comma_price.sum())
print("\nitem_status distinct:\n", items["item_status"].value_counts(dropna=False))
print("\nproduct_code case variants sample:", items[items["product_code"].str.upper() != items["product_code"]]["product_code"].unique()[:10])
print("\nproduct codes not matching P#### pattern:", items[~items["product_code"].str.match(r"^[Pp]\d{4}$", na=False)]["product_code"].unique())
# orphan check vs product catalog
valid_products = set(dfp["product_id"].str.upper())
orphan_products = set(items["product_code"].str.upper()) - valid_products
print("\nproduct codes in items NOT in product catalog:", orphan_products)
# orphan check vs orders
valid_orders = set(orders["order_id"].str.upper())
orphan_orders_in_items = set(items["order_id"].str.upper()) - valid_orders
print("order_ids in items NOT in orders header:", orphan_orders_in_items)

# ---------------------------------------------------------------
h("4. legado_regioes_pipe.txt")
regions = pd.read_csv(f"{SRC}/legado_regioes_pipe.txt", sep="|", dtype=str)
print(regions)
print("\nregional_code duplicates (case-insensitive):")
print(regions[regions["regional_code"].str.upper().duplicated(keep=False)])

# ---------------------------------------------------------------
h("5. vendedores.csv")
sellers = pd.read_csv(f"{SRC}/vendedores.csv", sep=";", dtype=str)
print("rows:", len(sellers))
print("\nseller_id duplicates:")
print(sellers[sellers["seller_id"].duplicated(keep=False)].sort_values("seller_id"))
print("\nstatus distinct:\n", sellers["status"].value_counts(dropna=False))
print("\nregional_code distinct (raw):", sorted(sellers["regional_code"].dropna().unique()))
print("\ncanal_id distinct (raw):", sorted(sellers["canal_id"].dropna().unique()))
print("\nnull canal_id count:", sellers["canal_id"].isna().sum())
print("\nnull regional_code count:", sellers["regional_code"].isna().sum())
print("\nhire_date sample formats:", sellers["hire_date"].dropna().sample(8, random_state=1).tolist())

# ---------------------------------------------------------------
h("6. logistica_entregas.json")
deliveries = json.load(open(f"{SRC}/logistica_entregas.json", encoding="utf-8"))
drows = []
for d in deliveries:
    drows.append({
        "delivery_id": d["delivery_id"],
        "order_ref": d["order_ref"],
        "carrier_name": d["carrier"]["name"],
        "carrier_mode": d["carrier"]["mode"],
        "delivery_status": d["delivery_status"],
        "shipped_at": d["timestamps"]["shipped_at"],
        "delivered_at": d["timestamps"]["delivered_at"],
        "state": d["destination"]["state"],
        "city": d["destination"]["city"],
        "cost": d["cost"],
    })
dfd = pd.DataFrame(drows)
print("rows:", len(dfd))
print("\ndelivery_id duplicates:")
print(dfd[dfd["delivery_id"].duplicated(keep=False)].sort_values("delivery_id"))
print("\ndelivery_status distinct:\n", dfd["delivery_status"].value_counts(dropna=False))
print("\ncarrier_mode distinct:\n", dfd["carrier_mode"].value_counts(dropna=False))
print("\nnull carrier_name count:", dfd["carrier_name"].isna().sum())
print("\ncost non-numeric ('unknown' etc):")
def is_bad_cost(v):
    try:
        float(v)
        return False
    except Exception:
        return True
print(dfd[dfd["cost"].apply(is_bad_cost)][["delivery_id","order_ref","cost"]])
print("\nstate distinct raw values (shows non-standard state names):")
print(sorted(dfd["state"].unique()))
print("\norder_ref referencing non-existent order O99999:", (dfd["order_ref"] == "O99999").sum())
orphan_orders_in_deliveries = set(dfd["order_ref"].str.upper()) - valid_orders
print("order_refs in deliveries NOT in orders header:", orphan_orders_in_deliveries)
print("\ninvalid date 31/02/2025 (Feb 31st doesn't exist):")
print(dfd[dfd["shipped_at"].str.contains("31/02/2025", na=False)])

# ---------------------------------------------------------------
h("7. atendimento_ocorrencias.ndjson")
tickets = [json.loads(l) for l in open(f"{SRC}/atendimento_ocorrencias.ndjson", encoding="utf-8") if l.strip()]
dft = pd.DataFrame(tickets)
print("rows:", len(dft))
print("columns:", list(dft.columns))
print("\nticket_id duplicates:", dft["ticket_id"].duplicated().sum())
print("\nevent_type distinct:\n", dft["event_type"].value_counts(dropna=False))
print("\nseverity distinct:\n", dft["severity"].value_counts(dropna=False))
print("\nstatus distinct:\n", dft["status"].value_counts(dropna=False))
print("\ncreated_at sample formats:", dft["created_at"].dropna().sample(10, random_state=1).tolist())
orphan_orders_in_tickets = set(dft["order_id"].astype(str).str.upper()) - valid_orders
print("\norder_ids in tickets NOT in orders header:", orphan_orders_in_tickets)
print("\nnull order_id count:", dft["order_id"].isna().sum())

# ---------------------------------------------------------------
h("8. comercial_canais.xlsx")
canais = pd.read_excel(f"{SRC}/comercial_canais.xlsx")
print(canais)
print("\nativo distinct raw:", canais["ativo"].unique())
print("id_canal distinct:", sorted(canais["id_canal"].unique()))

# ---------------------------------------------------------------
h("9. crm_clientes_export.xlsx")
clientes = pd.read_excel(f"{SRC}/crm_clientes_export.xlsx")
print("rows:", len(clientes))
print("columns:", list(clientes.columns))
print("\ncustomer_id duplicates:")
print(clientes[clientes["customer_id"].duplicated(keep=False)].sort_values("customer_id"))
print("\nstatus_cliente distinct:\n", clientes["status_cliente"].value_counts(dropna=False))
print("\nsegmento distinct:\n", clientes["segmento"].value_counts(dropna=False))
print("\nporte distinct:\n", clientes["porte"].value_counts(dropna=False))
print("\nestado distinct raw values:")
print(sorted(clientes["estado"].dropna().unique()))
print("\ndata_cadastro sample formats:", clientes["data_cadastro"].dropna().astype(str).sample(10, random_state=1).tolist())
print("\nnull email count:", clientes["email"].isna().sum())
print("\ninvalid email format sample:")
bad_email = ~clientes["email"].astype(str).str.match(r"^[^@]+@[^@]+\.[^@]+$", na=False)
print(clientes[bad_email][["customer_id","email"]].head(10))
# orphan check: customers referenced in orders but not in CRM
valid_customers = set(clientes["customer_id"].astype(str).str.upper())
orders_customer_up = orders["customer_code"].str.upper()
orphan_customers = set(orders_customer_up.unique()) - valid_customers
print("\ncustomer_codes in orders NOT in CRM customers:", orphan_customers)
