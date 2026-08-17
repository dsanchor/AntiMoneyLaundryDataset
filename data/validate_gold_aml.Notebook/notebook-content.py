# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9795a299-89c3-47fd-b94a-a8707bd0b534",
# META       "default_lakehouse_name": "raw_data",
# META       "default_lakehouse_workspace_id": "cc9a739c-0800-4ee7-b285-f487739752e5",
# META       "known_lakehouses": [
# META         {
# META           "id": "9795a299-89c3-47fd-b94a-a8707bd0b534"
# META         },
# META         {
# META           "id": "2fe1e17e-8415-4e6b-943b-067ad4598170"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

import json
from pyspark.sql import functions as F

WS_ID   = "cc9a739c-0800-4ee7-b285-f487739752e5"
GOLD_ID = "2fe1e17e-8415-4e6b-943b-067ad4598170"
GOLD_TABLES = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{GOLD_ID}/Tables"

tables = [
    "fact_transaction", "fact_laundering_pattern_txn", "dim_account", "dim_bank",
    "dim_currency", "dim_payment_format", "dim_date", "dim_laundering_pattern",
]
stats = {t: spark.read.format("delta").load(f"{GOLD_TABLES}/{t}").count() for t in tables}

tx = spark.read.format("delta").load(f"{GOLD_TABLES}/fact_transaction")
stats["_laundering_1"] = tx.filter(F.col("is_laundering") == 1).count()
stats["_with_pattern_type"] = tx.filter(F.col("pattern_type").isNotNull()).count()

out = "/lakehouse/default/Files/_gold_aml_stats.json"
with open(out, "w") as fh:
    json.dump(stats, fh, indent=2)

for k, v in stats.items():
    print(f"{k:32s} {v:,}")
print("written:", out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
