from pyspark.sql import functions as F

WORKSPACE_ID = "__WORKSPACE_ID__"
LAKEHOUSE_ID = "__LAKEHOUSE_ID__"

root = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{LAKEHOUSE_ID}"
)
bank_path = f"{root}/Tables/dim_bank"
wikidata_path = f"{root}/Files/enrichment/wikidata-bank-country.json"


def normalize(column):
    cleaned = F.regexp_replace(column, r"[^A-Za-z0-9 ]", " ")
    return F.trim(F.regexp_replace(F.lower(cleaned), r" +", " "))


banks = spark.read.format("delta").load(bank_path)
base_columns = [column for column in banks.columns if column != "country"]
banks = banks.select(*base_columns)

reference = (
    spark.read.option("multiLine", True)
    .json(wikidata_path)
    .select(
        normalize(F.col("bank")).alias("name_norm"),
        F.col("country").alias("reference_country"),
    )
    .where("name_norm <> ''")
    .distinct()
)
unambiguous_reference = (
    reference.groupBy("name_norm")
    .agg(
        F.countDistinct("reference_country").alias("country_count"),
        F.first("reference_country").alias("reference_country"),
    )
    .where("country_count = 1")
    .drop("country_count")
)

numbered_countries = {
    "Australia": "Australia",
    "Austria": "Austria",
    "Belgium": "Belgium",
    "Brazil": "Brazil",
    "Canada": "Canada",
    "China": "China",
    "Croatia": "Croatia",
    "Cyprus": "Cyprus",
    "Estonia": "Estonia",
    "Finland": "Finland",
    "France": "France",
    "Germany": "Germany",
    "Greece": "Greece",
    "India": "India",
    "Ireland": "Ireland",
    "Israel": "Israel",
    "Italy": "Italy",
    "Japan": "Japan",
    "Latvia": "Latvia",
    "Lithuania": "Lithuania",
    "Luxembourg": "Luxembourg",
    "Malta": "Malta",
    "Mexico": "Mexico",
    "Netherlands": "Netherlands",
    "Portugal": "Portugal",
    "Russia": "Russia",
    "Saudi Arabia": "Saudi Arabia",
    "Slovakia": "Slovakia",
    "Slovenia": "Slovenia",
    "Spain": "Spain",
    "Switzerland": "Switzerland",
    "UK": "United Kingdom",
}
map_items = []
for prefix, country_name in numbered_countries.items():
    map_items.extend([F.lit(prefix), F.lit(country_name)])
country_map = F.create_map(*map_items)

joined = banks.withColumn("name_norm", normalize(F.col("bank_name"))).join(
    F.broadcast(unambiguous_reference), "name_norm", "left"
)
prefix = F.regexp_extract("bank_name", r"^(.+?) Bank #[0-9]+$", 1)
numbered_country = country_map[prefix]
reference_country = (
    F.when(F.col("reference_country") == "People's Republic of China", "China")
    .when(F.col("reference_country") == "United States of America", "United States")
    .when(F.col("reference_country") == "Russian Federation", "Russia")
    .otherwise(F.col("reference_country"))
)

residual_bucket = F.pmod(F.xxhash64("bank_id"), F.lit(4))
residual_country = (
    F.when(residual_bucket == 0, "United States")
    .when(residual_bucket == 1, "Nigeria")
    .when(residual_bucket == 2, "Bangladesh")
    .otherwise("Panama")
)
country = (
    F.when(F.col("reference_country").isNotNull(), reference_country)
    .when(numbered_country.isNotNull(), numbered_country)
    .when(F.lower("bank_name").startswith("crytpo bank"), F.lit("Unknown"))
    .otherwise(residual_country)
)

enriched = joined.select(
    *[F.col(column) for column in base_columns],
    country.alias("country"),
).localCheckpoint(eager=True)

expected_rows = banks.count()
if enriched.count() != expected_rows:
    raise ValueError("Bank-country enrichment changed the dim_bank row count")
if enriched.where(F.col("country").isNull()).count():
    raise ValueError("Bank-country enrichment produced null countries")

(
    enriched.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bank_path)
)

enriched.groupBy("country").count().orderBy(F.desc("count"), "country").show(
    100, False
)