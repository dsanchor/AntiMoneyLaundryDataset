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

# MARKDOWN ********************

# # Construcción capa **Gold** — Estrella AML (IBM Transactions for Anti-Money Laundering)
# 
# **Fuente (Kaggle):** IBM Transactions for Anti-Money Laundering (AML)
# https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
# 
# Se construye un **modelo en estrella** en el lakehouse `Gold` a partir de los ficheros del
# **segmento HI-Medium** (según lo pedido) alojados en `raw_data/Files/AML_unzipped/`:
# 
# - `HI-Medium_Trans.csv`     → transacciones (hecho principal)
# - `HI-Medium_accounts.csv`  → catálogo de cuentas / entidades / bancos (dimensiones)
# - `HI-Medium_Patterns.txt`  → transacciones etiquetadas por tipología de blanqueo (hecho de patrones)
# 
# ## Esquema en estrella resultante (tablas Delta en `Gold`)
# 
# **Hechos**
# - `fact_transaction` — grano = 1 transacción. Métricas: `amount_received`, `amount_paid`.
#   Flags: `is_laundering`, `is_self_transfer`. FK: cuentas (from/to), fecha, divisas, formato,
#   `pattern_type` (enriquecido cruzando con Patterns).
# - `fact_laundering_pattern_txn` — grano = paso de transacción dentro de un intento de blanqueo
#   (bloque `BEGIN/END LAUNDERING ATTEMPT` de Patterns). Incluye `attempt_id`, `step_in_attempt`,
#   `pattern_type`.
# 
# **Dimensiones**
# - `dim_account` (PK `account_number`) → snowflake a `dim_bank`
# - `dim_bank` (PK `bank_id`)
# - `dim_date` (PK `date_key`)
# - `dim_currency` (PK `currency`)
# - `dim_payment_format` (PK `payment_format`)
# - `dim_laundering_pattern` (PK `pattern_type`)


# CELL ********************

# Parámetros y rutas
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType,
)

WS_ID   = "cc9a739c-0800-4ee7-b285-f487739752e5"
RAW_ID  = "9795a299-89c3-47fd-b94a-a8707bd0b534"   # lakehouse raw_data (default)
GOLD_ID = "2fe1e17e-8415-4e6b-943b-067ad4598170"   # lakehouse Gold (destino)

# Segmento de tamaño solicitado. Cambiar a "LI-Medium", "HI-Small", etc. para otros segmentos.
SEGMENT = "HI-Medium"

RAW_FILES   = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{RAW_ID}/Files/AML_unzipped"
GOLD_TABLES = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{GOLD_ID}/Tables"

TRANS_PATH    = f"{RAW_FILES}/{SEGMENT}_Trans.csv"
ACCOUNTS_PATH = f"{RAW_FILES}/{SEGMENT}_accounts.csv"
# raw_data es el lakehouse por defecto -> disponible por FUSE en /lakehouse/default/Files
PATTERNS_PATH_LOCAL = f"/lakehouse/default/Files/AML_unzipped/{SEGMENT}_Patterns.txt"

print("Segmento:", SEGMENT)
print("Trans   :", TRANS_PATH)
print("Accounts:", ACCOUNTS_PATH)
print("Patterns:", PATTERNS_PATH_LOCAL)
print("Destino :", GOLD_TABLES)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1) Lectura de transacciones (esquema explícito: el CSV tiene 2 columnas "Account" duplicadas)
trans_schema = StructType([
    StructField("ts_str",             StringType(),  True),
    StructField("from_bank_id",       StringType(),  True),
    StructField("from_account",       StringType(),  True),
    StructField("to_bank_id",         StringType(),  True),
    StructField("to_account",         StringType(),  True),
    StructField("amount_received",    DoubleType(),  True),
    StructField("receiving_currency", StringType(),  True),
    StructField("amount_paid",        DoubleType(),  True),
    StructField("payment_currency",   StringType(),  True),
    StructField("payment_format",     StringType(),  True),
    StructField("is_laundering",      IntegerType(), True),
])

trans = (
    spark.read.option("header", True).schema(trans_schema).csv(TRANS_PATH)
    .withColumn("txn_ts", F.to_timestamp("ts_str", "yyyy/MM/dd HH:mm"))
    .filter(F.col("from_account").isNotNull() & F.col("to_account").isNotNull())
)
trans.cache()
n_trans = trans.count()
print(f"Transacciones {SEGMENT}: {n_trans:,}")
trans.show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) Lectura del catálogo de cuentas
accounts = (
    spark.read.option("header", True).csv(ACCOUNTS_PATH)
    .toDF("bank_name", "bank_id", "account_number", "entity_id", "entity_name")
    .filter(F.col("account_number").isNotNull())
    .dropDuplicates(["account_number"])
)
accounts.cache()
print(f"Cuentas en catálogo: {accounts.count():,}")
accounts.show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3) Parseo de Patterns.txt -> filas de transacciones etiquetadas por tipología de blanqueo
#    Formato: bloques  BEGIN LAUNDERING ATTEMPT - <TIPO>[:  desc]  / <líneas trans> / END LAUNDERING ATTEMPT
pat_rows = []
attempt_id = 0
cur_type = None
step = 0
with open(PATTERNS_PATH_LOCAL, "r") as fh:
    for raw_line in fh:
        line = raw_line.rstrip("\n").rstrip("\r")
        if line.startswith("BEGIN LAUNDERING ATTEMPT"):
            attempt_id += 1
            after = line.split(" - ", 1)[1] if " - " in line else ""
            cur_type = after.split(":", 1)[0].strip().upper()
            step = 0
        elif line.startswith("END LAUNDERING ATTEMPT"):
            cur_type = None
        elif line.strip() == "":
            continue
        elif cur_type is not None:
            p = line.split(",")
            if len(p) >= 11:
                step += 1
                try:
                    pat_rows.append((
                        attempt_id, cur_type, step,
                        p[0], p[1], p[2], p[3], p[4],
                        float(p[5]), p[6], float(p[7]), p[8], p[9], int(p[10]),
                    ))
                except ValueError:
                    pass

print(f"Intentos de blanqueo: {attempt_id:,}  |  filas de transacción etiquetadas: {len(pat_rows):,}")

pat_schema = StructType([
    StructField("attempt_id",         IntegerType(), True),
    StructField("pattern_type",       StringType(),  True),
    StructField("step_in_attempt",    IntegerType(), True),
    StructField("ts_str",             StringType(),  True),
    StructField("from_bank_id",       StringType(),  True),
    StructField("from_account",       StringType(),  True),
    StructField("to_bank_id",         StringType(),  True),
    StructField("to_account",         StringType(),  True),
    StructField("amount_received",    DoubleType(),  True),
    StructField("receiving_currency", StringType(),  True),
    StructField("amount_paid",        DoubleType(),  True),
    StructField("payment_currency",   StringType(),  True),
    StructField("payment_format",     StringType(),  True),
    StructField("is_laundering",      IntegerType(), True),
])
patterns = (
    spark.createDataFrame(pat_rows, schema=pat_schema)
    .withColumn("txn_ts", F.to_timestamp("ts_str", "yyyy/MM/dd HH:mm"))
)
patterns.cache()
patterns.groupBy("pattern_type").count().orderBy(F.desc("count")).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4) DIMENSIONES

# 4.1 dim_bank (desde el catálogo de cuentas)
dim_bank = (
    accounts.select("bank_id", "bank_name")
    .filter(F.col("bank_id").isNotNull())
    .dropDuplicates(["bank_id"])
)

# 4.2 dim_account = catálogo + cuentas vistas en Trans no presentes en el catálogo (integridad FK)
accts_in_trans = (
    trans.select(F.col("from_account").alias("account_number"), F.col("from_bank_id").alias("bank_id"))
    .union(trans.select(F.col("to_account").alias("account_number"), F.col("to_bank_id").alias("bank_id")))
    .filter(F.col("account_number").isNotNull())
    .dropDuplicates(["account_number"])
)
missing_accts = (
    accts_in_trans.join(accounts.select("account_number"), "account_number", "left_anti")
    .withColumn("bank_name", F.lit(None).cast("string"))
    .withColumn("entity_id", F.lit(None).cast("string"))
    .withColumn("entity_name", F.lit("UNKNOWN"))
    .select("bank_name", "bank_id", "account_number", "entity_id", "entity_name")
)
dim_account = (
    accounts.withColumn("in_accounts_master", F.lit(True))
    .unionByName(missing_accts.withColumn("in_accounts_master", F.lit(False)))
    .dropDuplicates(["account_number"])
)

# 4.3 dim_currency (unión de divisas de cobro y pago, trans + patterns)
cur = (
    trans.select(F.col("receiving_currency").alias("currency"))
    .union(trans.select(F.col("payment_currency").alias("currency")))
    .union(patterns.select(F.col("receiving_currency").alias("currency")))
    .union(patterns.select(F.col("payment_currency").alias("currency")))
    .filter(F.col("currency").isNotNull())
    .dropDuplicates(["currency"])
)
dim_currency = cur.withColumn(
    "is_crypto", F.when(F.lower(F.col("currency")).contains("bitcoin"), F.lit(True)).otherwise(F.lit(False))
)

# 4.4 dim_payment_format
dim_payment_format = (
    trans.select("payment_format")
    .union(patterns.select("payment_format"))
    .filter(F.col("payment_format").isNotNull())
    .dropDuplicates(["payment_format"])
)

# 4.5 dim_date (grano día)
dim_date = (
    trans.select(F.to_date("txn_ts").alias("date"))
    .union(patterns.select(F.to_date("txn_ts").alias("date")))
    .filter(F.col("date").isNotNull())
    .dropDuplicates(["date"])
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast(IntegerType()))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
)

# 4.6 dim_laundering_pattern
dim_laundering_pattern = (
    patterns.select("pattern_type")
    .filter(F.col("pattern_type").isNotNull())
    .dropDuplicates(["pattern_type"])
)

for name, df in [
    ("dim_bank", dim_bank), ("dim_account", dim_account), ("dim_currency", dim_currency),
    ("dim_payment_format", dim_payment_format), ("dim_date", dim_date),
    ("dim_laundering_pattern", dim_laundering_pattern),
]:
    print(f"{name:24s} filas = {df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5) HECHOS

# 5.1 Clave natural de patrón por transacción (para enriquecer fact_transaction)
pat_key_cols = ["ts_str", "from_account", "to_account", "amount_paid", "payment_format"]
pattern_lookup = (
    patterns.select(*pat_key_cols, "pattern_type", "attempt_id")
    .dropDuplicates(pat_key_cols)
)

# 5.2 fact_transaction (grano = transacción)
fact_transaction = (
    trans
    .withColumn("date_key", F.date_format("txn_ts", "yyyyMMdd").cast(IntegerType()))
    .withColumn("txn_hour", F.hour("txn_ts"))
    .withColumn("is_self_transfer", F.col("from_account") == F.col("to_account"))
    .join(pattern_lookup, pat_key_cols, "left")
    .withColumn("transaction_sk", F.monotonically_increasing_id())
    .select(
        "transaction_sk", "date_key", "txn_ts", "txn_hour",
        "from_bank_id", "from_account", "to_bank_id", "to_account",
        "amount_received", "receiving_currency", "amount_paid", "payment_currency",
        "payment_format", "is_laundering", "is_self_transfer",
        "pattern_type", "attempt_id",
    )
)

# 5.3 fact_laundering_pattern_txn (grano = paso dentro de un intento de blanqueo)
fact_laundering_pattern_txn = (
    patterns
    .withColumn("date_key", F.date_format("txn_ts", "yyyyMMdd").cast(IntegerType()))
    .withColumn("pattern_txn_sk", F.monotonically_increasing_id())
    .select(
        "pattern_txn_sk", "attempt_id", "pattern_type", "step_in_attempt",
        "date_key", "txn_ts",
        "from_bank_id", "from_account", "to_bank_id", "to_account",
        "amount_received", "receiving_currency", "amount_paid", "payment_currency",
        "payment_format", "is_laundering",
    )
)

print("fact_transaction            filas =", f"{fact_transaction.count():,}")
print("fact_laundering_pattern_txn filas =", f"{fact_laundering_pattern_txn.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6) Escritura de las tablas Delta en el lakehouse Gold
def write_gold(df, name):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .save(f"{GOLD_TABLES}/{name}"))
    print(f"  ✔ {name}")

print("Escribiendo dimensiones...")
write_gold(dim_bank, "dim_bank")
write_gold(dim_account, "dim_account")
write_gold(dim_currency, "dim_currency")
write_gold(dim_payment_format, "dim_payment_format")
write_gold(dim_date, "dim_date")
write_gold(dim_laundering_pattern, "dim_laundering_pattern")

print("Escribiendo hechos...")
write_gold(fact_transaction, "fact_transaction")
write_gold(fact_laundering_pattern_txn, "fact_laundering_pattern_txn")

print("Capa Gold construida.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 7) Validación
tx = spark.read.format("delta").load(f"{GOLD_TABLES}/fact_transaction")
total = tx.count()
laund = tx.filter(F.col("is_laundering") == 1).count()
matched = tx.filter(F.col("pattern_type").isNotNull()).count()

acc = spark.read.format("delta").load(f"{GOLD_TABLES}/dim_account")
master = acc.filter(F.col("in_accounts_master") == True).count()
unknown = acc.filter(F.col("in_accounts_master") == False).count()

print(f"fact_transaction total           : {total:,}")
print(f"  · marcadas is_laundering=1      : {laund:,} ({100.0*laund/total:.4f}%)")
print(f"  · con pattern_type (de Patterns): {matched:,}")
print(f"dim_account total                : {acc.count():,}  (master={master:,}, unknown/solo-trans={unknown:,})")

print("\nTop tipologías de blanqueo (fact_laundering_pattern_txn):")
(spark.read.format("delta").load(f"{GOLD_TABLES}/fact_laundering_pattern_txn")
    .groupBy("pattern_type").count().orderBy(F.desc("count")).show(truncate=False))

print("Divisas (dim_currency):")
spark.read.format("delta").load(f"{GOLD_TABLES}/dim_currency").orderBy("currency").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
