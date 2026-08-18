from notebookutils import mssparkutils
import json

SOURCE_WORKSPACE_ID = "__WORKSPACE_ID__"
SOURCE_LAKEHOUSE_ID = "__LAKEHOUSE_ID__"

tables_root = (
    f"abfss://{SOURCE_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{SOURCE_LAKEHOUSE_ID}/Tables"
)
snapshot_root = (
    f"abfss://{SOURCE_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{SOURCE_LAKEHOUSE_ID}/Files/github_exports/gold"
)

table_partitions = {
    "dim_account": 2,
    "dim_bank": 1,
    "dim_currency": 1,
    "dim_date": 1,
    "dim_laundering_pattern": 1,
    "dim_payment_format": 1,
    "fact_laundering_pattern_txn": 1,
    "fact_transaction": 32,
}

manifest_tables = []
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")

for name, partitions in table_partitions.items():
    source = f"{tables_root}/{name}"
    target = f"{snapshot_root}/{name}"
    frame = spark.read.format("delta").load(source)
    if name == "dim_bank" and "country" not in frame.columns:
        raise ValueError("dim_bank must contain country before exporting the snapshot")
    row_count = frame.count()

    (frame.repartition(partitions)
        .write.format("parquet")
        .mode("overwrite")
        .option("compression", "zstd")
        .save(target))

    exported_count = spark.read.format("parquet").load(target).count()
    if exported_count != row_count:
        raise ValueError(f"{name}: exported {exported_count} rows; expected {row_count}")

    manifest_tables.append({"name": name, "expectedRows": row_count})
    print(f"exported {name}: {row_count} rows")

manifest = {
    "formatVersion": 1,
    "snapshot": "aml-gold-2026",
    "storageFormat": "parquet",
    "compression": "zstd",
    "tables": manifest_tables,
}
mssparkutils.fs.put(
    f"{snapshot_root}/manifest.json",
    json.dumps(manifest, indent=2, sort_keys=True),
    True,
)
print("EXPORT_RESULT_JSON=" + json.dumps(manifest, sort_keys=True))
