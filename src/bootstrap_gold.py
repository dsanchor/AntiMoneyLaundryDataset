from notebookutils import mssparkutils
from pyspark.sql import functions as F
import json

WORKSPACE_ID = "__WORKSPACE_ID__"
LAKEHOUSE_ID = "__LAKEHOUSE_ID__"

snapshot_root = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{LAKEHOUSE_ID}/Files/bootstrap/gold"
)
tables_root = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{LAKEHOUSE_ID}/Tables"
)

manifest = json.loads(mssparkutils.fs.head(f"{snapshot_root}/manifest.json", 1024 * 1024))

spark.conf.set("spark.sql.parquet.vorder.default", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

results = []
for table in manifest["tables"]:
    name = table["name"]
    expected_rows = int(table["expectedRows"])
    source = f"{snapshot_root}/{name}"
    target = f"{tables_root}/{name}"

    frame = spark.read.format("parquet").load(source)
    source_rows = frame.count()
    if source_rows != expected_rows:
        raise ValueError(f"{name}: snapshot has {source_rows} rows; expected {expected_rows}")

    (frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target))

    deployed_rows = spark.read.format("delta").load(target).count()
    if deployed_rows != expected_rows:
        raise ValueError(f"{name}: deployed {deployed_rows} rows; expected {expected_rows}")

    results.append({"table": name, "rows": deployed_rows})

transaction = spark.read.format("delta").load(f"{tables_root}/fact_transaction")
patterns = spark.read.format("delta").load(f"{tables_root}/fact_laundering_pattern_txn")
dates = spark.read.format("delta").load(f"{tables_root}/dim_date")

date_key_mismatches = transaction.where(
    F.col("date_key") != F.date_format("txn_ts", "yyyyMMdd").cast("int")
).count()
missing_dates = transaction.select("date_key").distinct().join(
    dates.select("date_key"), "date_key", "left_anti"
).count()
orphan_patterns = patterns.select("pattern_txn_sk").join(
    transaction.select("transaction_sk"),
    F.col("pattern_txn_sk") == F.col("transaction_sk"),
    "left_anti",
).count()

if date_key_mismatches or missing_dates or orphan_patterns:
    raise ValueError(
        "Post-deployment validation failed: "
        f"date_key_mismatches={date_key_mismatches}, "
        f"missing_dates={missing_dates}, orphan_patterns={orphan_patterns}"
    )

output = {
    "status": "ok",
    "workspaceId": WORKSPACE_ID,
    "lakehouseId": LAKEHOUSE_ID,
    "tables": results,
    "dateKeyMismatches": date_key_mismatches,
    "missingDates": missing_dates,
    "orphanPatterns": orphan_patterns,
}
print("DEPLOY_RESULT_JSON=" + json.dumps(output, sort_keys=True))
