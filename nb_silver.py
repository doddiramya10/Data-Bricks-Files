# Databricks notebook source
# DBTITLE 1,Silver cleansing and dedup
# Databricks notebook source
import json
from functools import reduce
from pyspark.sql import functions as F, Window

DEFAULT_CONFIG_PATH = "/Workspace/Users/nandinihazare208@gmail.com/config/sources.json"
dbutils.widgets.text("config_path", DEFAULT_CONFIG_PATH)
config_path = dbutils.widgets.get("config_path").strip() or DEFAULT_CONFIG_PATH
with open(config_path, encoding="utf-8") as handle:
    cfg = json.load(handle)
catalog = cfg["catalog"]
for source in cfg["sources"]:
    name = source["name"]
    df = spark.table(f"{catalog}.bronze.{name}")
    keys = source["primary_keys"]
    assert keys and all(k in df.columns for k in keys), f"Missing keys: {name}"
    invalid = reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])
    for col in source.get("required_columns", []):
        invalid = invalid | F.col(col).isNull()
    for col in source.get("positive_columns", []):
        invalid = invalid | (F.col(col) <= 0)
    if "_rescued_data" in df.columns:
        invalid = invalid | F.col("_rescued_data").isNotNull()
    (df.filter(invalid).withColumn("_quality_reason", F.lit("invalid_key_required_value_or_rescued_data"))
     .write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.quarantine.{name}"))
    valid = df.filter(~invalid)
    order = []
    if source.get("sequence_column"):
        assert source["sequence_column"] in df.columns
        order.append(F.col(source["sequence_column"]).desc_nulls_last())
    order.extend([F.col("_ingested_at").desc(),
                  F.sha2(F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)])), 256).desc()])
    clean = (valid.withColumn("_row_number", F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))
             .filter(F.col("_row_number") == 1).drop("_row_number"))
    # Rebuild the current clean view; this also removes stale quarantined keys.
    clean.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.{name}")
