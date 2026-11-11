# Databricks notebook source
# Databricks notebook source
import json
from functools import reduce
from pyspark.sql import functions as F

dbutils.widgets.text("config_path", "")
with open(dbutils.widgets.get("config_path"), encoding="utf-8") as handle:
    cfg = json.load(handle)
catalog = cfg["catalog"]
metrics = []
for source in cfg["sources"]:
    name = source["name"]
    bronze = spark.table(f"{catalog}.bronze.{name}")
    silver = spark.table(f"{catalog}.silver.{name}")
    quarantine = spark.table(f"{catalog}.quarantine.{name}")
    row = (bronze.agg(F.count("*").alias("bronze_rows"), F.max("_ingested_at").alias("latest_ingestion"))
           .crossJoin(silver.agg(F.count("*").alias("silver_rows")))
           .crossJoin(quarantine.agg(F.count("*").alias("quarantined_rows")))
           .withColumn("entity", F.lit(name)).withColumn("refreshed_at", F.current_timestamp()))
    metrics.append(row)
reduce(lambda a, b: a.unionByName(b), metrics).write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.gold.pipeline_quality_summary")
display(spark.table(f"{catalog}.gold.pipeline_quality_summary"))

# COMMAND ----------
# Retail demo: require all dimensions to match before publishing sales totals.
if cfg.get("demo_retail_gold"):
    orders = spark.table(f"{catalog}.silver.orders").alias("o")
    customers = spark.table(f"{catalog}.silver.customers").alias("c")
    products = spark.table(f"{catalog}.silver.products").alias("p")
    joined = (orders.join(customers, F.col("o.customer_id") == F.col("c.customer_id"), "left")
              .join(products, F.col("o.product_id") == F.col("p.product_id"), "left"))
    assert joined.filter(F.col("c.customer_id").isNull() | F.col("p.product_id").isNull()).limit(1).count() == 0, "Unmatched customer/product references"
    sales = joined.select("o.order_id", "o.order_date", "o.customer_id", "c.customer_name", "c.city",
                          "o.product_id", "p.product_name", "p.category", "o.quantity", "p.unit_price",
                          (F.col("o.quantity") * F.col("p.unit_price")).cast("decimal(18,2)").alias("revenue"))
    sales.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.order_sales")
    (sales.groupBy("order_date").agg(F.count("*").alias("orders"), F.sum("quantity").alias("units"),
                                   F.sum("revenue").alias("revenue"))
     .write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.daily_sales"))
    (sales.groupBy("customer_id", "customer_name", "city")
     .agg(F.count("*").alias("orders"), F.sum("revenue").alias("revenue"))
     .write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.customer_sales"))
    display(spark.table(f"{catalog}.gold.daily_sales"))
