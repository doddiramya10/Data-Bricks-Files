# Databricks notebook source
# Databricks notebook source
import json
import re
from pyspark.sql import functions as F

DEFAULT_CONFIG_PATH = "/Workspace/Users/nandinihazare208@gmail.com/config/sources.json"
dbutils.widgets.text("config_path", DEFAULT_CONFIG_PATH)
config_path = dbutils.widgets.get("config_path").strip() or DEFAULT_CONFIG_PATH
with open(config_path, encoding="utf-8") as handle:
    cfg = json.load(handle)
assert "REPLACE_" not in json.dumps(cfg), "Complete config/sources.json before running"
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cfg["catalog"])
catalog = cfg["catalog"]
for auth in cfg.get("storage_auth", []):
    spark.conf.set(f"fs.azure.account.key.{auth['account']}.{auth['endpoint']}",
                   dbutils.secrets.get(auth["scope"], auth["key"]))
sources = cfg["sources"]
assert sources and len({s["name"] for s in sources}) == len(sources)
for layer in ("bronze", "silver", "gold", "quarantine"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{layer}`")
if cfg["checkpoint_root"].startswith("/Volumes/"):
    volume_catalog, volume_schema, volume_name = cfg["checkpoint_root"].split("/")[2:5]
    assert all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in (volume_catalog, volume_schema, volume_name))
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{volume_catalog}`.`{volume_schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{volume_catalog}`.`{volume_schema}`.`{volume_name}`")
for source in sources:
    name = source["name"]
    assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name), name
    target = f"{catalog}.bronze.{name}"
    if source["kind"] == "files":
        state = f"{cfg['checkpoint_root'].rstrip('/')}/{name}"
        df = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", source["format"])
              .option("cloudFiles.schemaLocation", state + "/schema")
              .option("cloudFiles.schemaEvolutionMode", "rescue")
              .option("rescuedDataColumn", "_rescued_data")
              .options(**source.get("options", {}))
              .schema(source["schema"]).load(source["path"]))
        assert not {"_ingested_at", "_source_file", "_source"}.intersection(df.columns)
        df = (df.withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source", F.lit(name)))
        (df.writeStream.format("delta").outputMode("append")
         .option("checkpointLocation", state + "/checkpoint")
         .trigger(availableNow=True).toTable(target).awaitTermination())
    elif source["kind"] == "sqlserver":
        # Full current snapshot: reruns are idempotent and source deletions propagate.
        df = (spark.read.format("jdbc")
              .option("url", source["jdbc_url"]).option("dbtable", source["table"])
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
              .option("user", dbutils.secrets.get(source["secret_scope"], source["username_key"]))
              .option("password", dbutils.secrets.get(source["secret_scope"], source["password_key"]))
              .load())
        assert not {"_ingested_at", "_source_file", "_source"}.intersection(df.columns)
        (df.withColumn("_source", F.lit(name)).withColumn("_ingested_at", F.current_timestamp())
         .write.format("delta").mode("overwrite").saveAsTable(target))
    else:
        raise ValueError(f"Unsupported source kind: {source['kind']}")


# COMMAND ----------

