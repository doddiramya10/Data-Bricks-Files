# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Multi-Source Delta Ingestion to Unity Catalog
# MAGIC
# MAGIC Ingests files from Azure Blob Storage and ADLS Gen2 (staged into a UC Volume) and tables from Azure SQL into Delta tables in `retail_catalog01.bronze`. Run the cells top to bottom.

# COMMAND ----------

# DBTITLE 1,Multi-source ingestion (combined)
# ================================================================
# AZURE DATABRICKS - END-TO-END MULTI-SOURCE DELTA INGESTION
# ================================================================
# SOURCES: Azure Blob Storage, ADLS Gen2, Azure SQL Database
# TARGET : Unity Catalog -> retail_catalog01.bronze -> Delta tables
# ================================================================

# ================================================================
# 1. IMPORTS
# ================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException

import os
import re
import uuid
import hashlib
import traceback
from collections import defaultdict

from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

# ================================================================
# 2. CONFIGURATION
# ================================================================
# Serverless compute does not allow spark.conf.set("fs.azure.account.key...").
# Files are copied from Blob / ADLS into a Unity Catalog Volume with the
# Azure Storage SDK, and Spark reads them from the Volume.

CATALOG_NAME = "retail_catalog01"
SCHEMA_NAME = "bronze"          # schema only (catalog is added separately)
LANDING_VOLUME = "landing"
LANDING_ROOT = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{LANDING_VOLUME}"

# For reruns, existing Delta tables are overwritten.
WRITE_MODE = "overwrite"

# Azure Blob Storage
BLOB_ACCOUNT = "storagebolb01"
BLOB_CONTAINER = "blob"
BLOB_LANDING_PATH = f"{LANDING_ROOT}/blob/"

# ADLS Gen2
ADLS_ACCOUNT = "adlsgenstorage01"
ADLS_CONTAINER = "adls"
ADLS_LANDING_PATH = f"{LANDING_ROOT}/adls/"

# Azure SQL Server
SQL_SERVER = "project01.database.windows.net"
SQL_DATABASE = "test"
SQL_PORT = 1433

JDBC_URL = (
    f"jdbc:sqlserver://{SQL_SERVER}:{SQL_PORT};"
    f"databaseName={SQL_DATABASE};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=30;"
)

print("Landing root:", LANDING_ROOT)
print("JDBC URL:", JDBC_URL)

# ================================================================
# 3. LOAD CREDENTIALS FROM DATABRICKS SECRETS
# ================================================================
# All credentials come from Databricks Secrets. Never paste keys into the notebook.

SECRET_SCOPE = "retail-secrets"

BLOB_ACCOUNT_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="blob-account-key")
ADLS_ACCOUNT_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="adls-account-key")
SQL_USERNAME = dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-username")
SQL_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-password")

JDBC_PROPERTIES = {
    "user": SQL_USERNAME,
    "password": SQL_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print("Credentials loaded from scope:", SECRET_SCOPE)

# ================================================================
# 4. CREATE CATALOG, SCHEMA AND LANDING VOLUME
# ================================================================

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG_NAME}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`{SCHEMA_NAME}`")
spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
spark.sql(f"USE SCHEMA `{SCHEMA_NAME}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG_NAME}`.`{SCHEMA_NAME}`.`{LANDING_VOLUME}`")

print("Unity Catalog created/verified successfully")
print("Catalog:", CATALOG_NAME)
print("Schema:", SCHEMA_NAME)
print("Landing volume:", LANDING_ROOT)

# ================================================================
# 5. STAGING FUNCTIONS (AZURE STORAGE -> UC VOLUME)
# ================================================================

STAGE_EXTENSIONS = (".csv", ".json", ".parquet", ".delta")

def _should_stage(name):
    # Keep supported files and every file inside a *.delta directory
    return any(
        part.lower().endswith(STAGE_EXTENSIONS)
        for part in name.split("/")
    )

def _write_local(target_path, downloader):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(target_path, "wb") as f:
        downloader.readinto(f)

def stage_blob_container():
    service = BlobServiceClient(
        account_url=f"https://{BLOB_ACCOUNT}.blob.core.windows.net",
        credential=BLOB_ACCOUNT_KEY
    )
    container = service.get_container_client(BLOB_CONTAINER)
    copied = 0
    for blob in container.list_blobs():
        if blob.name.endswith("/") or not _should_stage(blob.name):
            continue
        target = os.path.join(BLOB_LANDING_PATH, blob.name)
        _write_local(target, container.download_blob(blob.name))
        copied += 1
    print(f"Blob files staged: {copied}")

def stage_adls_filesystem():
    service = DataLakeServiceClient(
        account_url=f"https://{ADLS_ACCOUNT}.dfs.core.windows.net",
        credential=ADLS_ACCOUNT_KEY
    )
    fs_client = service.get_file_system_client(ADLS_CONTAINER)
    copied = 0
    for p in fs_client.get_paths(recursive=True):
        if p.is_directory or not _should_stage(p.name):
            continue
        target = os.path.join(ADLS_LANDING_PATH, p.name)
        _write_local(target, fs_client.get_file_client(p.name).download_file())
        copied += 1
    print(f"ADLS files staged: {copied}")

# ================================================================
# 6. STAGE BLOB AND ADLS FILES INTO THE VOLUME
# ================================================================

stage_blob_container()
stage_adls_filesystem()

# ================================================================
# 7. UTILITY FUNCTIONS
# ================================================================

def sanitize_name(name):
    name = name.lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")

    if not name:
        name = "unnamed"

    if name[0].isdigit():
        name = "t_" + name

    return name


def delta_table_name(source, filename):
    cleaned_name = sanitize_name(filename)
    return (
        f"`{CATALOG_NAME}`."
        f"`{SCHEMA_NAME}`."
        f"`{sanitize_name(source + '_' + cleaned_name)}`"
    )


def save_as_delta(df, table_name):
    print(f"\nWriting Delta table: {table_name}")
    (
        df.write
        .format("delta")
        .mode(WRITE_MODE)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"SUCCESS: {table_name}")

# ================================================================
# 8. RECURSIVELY DISCOVER STORAGE FILES
# ================================================================

SUPPORTED_EXTENSIONS = {".csv", ".json", ".parquet", ".delta"}

def list_all_files(path):
    files = []
    try:
        items = dbutils.fs.ls(path)
        for item in items:
            if item.isDir():
                # Delta directories are datasets, not ordinary folders
                if item.path.rstrip("/").endswith(".delta"):
                    files.append(item.path)
                else:
                    files.extend(list_all_files(item.path))
            else:
                filename = item.name.lower()
                # Ignore Spark metadata and temporary files
                if filename.startswith("_") or filename.startswith("."):
                    continue
                if any(filename.endswith(ext) for ext in SUPPORTED_EXTENSIONS):
                    files.append(item.path)
    except Exception as e:
        print(f"Unable to list path {path}: {str(e)}")
    return files

# ================================================================
# 9. READ DIFFERENT FILE FORMATS
# ================================================================

def get_file_format(path):
    lower_path = path.lower().rstrip("/")
    if lower_path.endswith(".csv"):
        return "csv"
    elif lower_path.endswith(".json"):
        return "json"
    elif lower_path.endswith(".parquet"):
        return "parquet"
    elif lower_path.endswith(".delta"):
        return "delta"
    return None


def read_file(path):
    file_format = get_file_format(path)

    if file_format == "csv":
        return (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "false")
            .load(path)
        )
    elif file_format == "json":
        return (
            spark.read
            .format("json")
            .option("multiLine", "false")
            .load(path)
        )
    elif file_format == "parquet":
        return spark.read.parquet(path)
    elif file_format == "delta":
        return spark.read.format("delta").load(path)
    else:
        raise ValueError(f"Unsupported file format: {path}")

# ================================================================
# 10. GENERATE UNIQUE TABLE NAMES
# ================================================================

used_table_names = set()

def generate_unique_name(source, path):
    relative_name = path.rstrip("/").split("/")[-1]

    for extension in SUPPORTED_EXTENSIONS:
        if relative_name.lower().endswith(extension):
            relative_name = relative_name[:-len(extension)]
            break

    base_name = sanitize_name(source + "_" + relative_name)
    candidate = base_name

    # Avoid collisions for same filename in different folders
    if candidate in used_table_names:
        suffix = hashlib.sha256(path.encode("utf-8")).hexdigest()[:8]
        candidate = f"{base_name}_{suffix}"

    used_table_names.add(candidate)

    return f"`{CATALOG_NAME}`.`{SCHEMA_NAME}`.`{candidate}`"

# ================================================================
# 11. STORAGE INGESTION FUNCTION
# ================================================================

ingestion_results = []

def ingest_storage_files(source_name, root_path):
    print("\n========================================")
    print(f"STARTING {source_name.upper()} INGESTION")
    print("========================================")

    files = list_all_files(root_path)
    print("Total discovered files:", len(files))

    for file_path in files:
        table_name = generate_unique_name(source_name, file_path)
        try:
            print("\nReading:", file_path)
            df = read_file(file_path)

            # Add audit metadata
            df = df.withColumns({
                "_ingestion_timestamp": F.current_timestamp(),
                "_source_system": F.lit(source_name),
                "_source_file": F.lit(file_path)
            })

            # saveAsTable is the action, so read/transform errors surface here
            save_as_delta(df, table_name)

            ingestion_results.append({
                "source": source_name,
                "source_path": file_path,
                "target_table": table_name,
                "status": "SUCCESS",
                "error": ""
            })
        except Exception as e:
            print("FAILED:", file_path)
            print(str(e))
            ingestion_results.append({
                "source": source_name,
                "source_path": file_path,
                "target_table": table_name,
                "status": "FAILED",
                "error": str(e)[:1000]
            })

# ================================================================
# 12. INGEST BLOB STORAGE FILES
# ================================================================

ingest_storage_files(source_name="blob", root_path=BLOB_LANDING_PATH)

# ================================================================
# 13. INGEST ADLS GEN2 FILES
# ================================================================

ingest_storage_files(source_name="adls", root_path=ADLS_LANDING_PATH)

# ================================================================
# 14. DISCOVER SQL SERVER TABLES
# ================================================================

SQL_TABLE_QUERY = """
(
    SELECT
        TABLE_SCHEMA,
        TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
) AS source_tables
"""

def get_sql_tables():
    return (
        spark.read
        .jdbc(url=JDBC_URL, table=SQL_TABLE_QUERY, properties=JDBC_PROPERTIES)
        .collect()
    )

# ================================================================
# 15. SQL SERVER INGESTION FUNCTION
# ================================================================

def ingest_sql_server():
    print("\n========================================")
    print("STARTING SQL SERVER INGESTION")
    print("========================================")

    try:
        sql_tables = get_sql_tables()
    except Exception as e:
        msg = str(e)
        hint = ""
        if "Client with IP address" in msg or "FAILED_JDBC.CONNECTION" in msg:
            hint = (
                " | Azure SQL firewall is blocking serverless compute. Enable "
                "'Allow Azure services and resources to access this server' on "
                f"{SQL_SERVER} (Networking), or allow the serverless NCC IPs."
            )
        print("FAILED to connect to SQL Server." + hint)
        ingestion_results.append({
            "source": "sql_server",
            "source_path": f"{SQL_SERVER}/{SQL_DATABASE}",
            "target_table": "",
            "status": "FAILED",
            "error": (msg[:800] + hint)
        })
        return

    print("SQL tables discovered:", len(sql_tables))

    for row in sql_tables:
        schema_name = row["TABLE_SCHEMA"]
        source_table = row["TABLE_NAME"]

        # Escape SQL Server identifiers
        safe_schema = schema_name.replace("]", "]]")
        safe_table = source_table.replace("]", "]]")
        sql_table_name = f"[{safe_schema}].[{safe_table}]"

        target_name = (
            f"`{CATALOG_NAME}`."
            f"`{SCHEMA_NAME}`."
            f"`{sanitize_name('sql_' + schema_name + '_' + source_table)}`"
        )

        try:
            print("\nReading SQL table:", sql_table_name)
            df = spark.read.jdbc(url=JDBC_URL, table=sql_table_name, properties=JDBC_PROPERTIES)

            df = df.withColumns({
                "_ingestion_timestamp": F.current_timestamp(),
                "_source_system": F.lit("sql_server"),
                "_source_table": F.lit(f"{schema_name}.{source_table}")
            })

            # saveAsTable is the action, so read/transform errors surface here
            save_as_delta(df, target_name)

            ingestion_results.append({
                "source": "sql_server",
                "source_path": sql_table_name,
                "target_table": target_name,
                "status": "SUCCESS",
                "error": ""
            })
        except Exception as e:
            print("FAILED SQL TABLE:", sql_table_name)
            print(str(e))
            ingestion_results.append({
                "source": "sql_server",
                "source_path": sql_table_name,
                "target_table": target_name,
                "status": "FAILED",
                "error": str(e)[:1000]
            })

# ================================================================
# 16. INGEST SQL SERVER TABLES
# ================================================================

ingest_sql_server()

# ================================================================
# 17. INGESTION SUMMARY
# ================================================================

print("========================================")
print("INGESTION SUMMARY")
print("========================================")

summary_df = None

if ingestion_results:
    summary_df = spark.createDataFrame(ingestion_results)
    display(summary_df)

    successful = sum(1 for x in ingestion_results if x["status"] == "SUCCESS")
    failed = sum(1 for x in ingestion_results if x["status"] == "FAILED")

    print("Total datasets:", len(ingestion_results))
    print("Successful:", successful)
    print("Failed:", failed)
else:
    print("No datasets processed.")

# ================================================================
# 18. SHOW DELTA TABLES IN SCHEMA
# ================================================================

tables_df = spark.sql(f"SHOW TABLES IN `{CATALOG_NAME}`.`{SCHEMA_NAME}`")
display(tables_df)

# ================================================================
# 19. VERIFY CREATED TABLES
# ================================================================

successful_tables = [
    item["target_table"]
    for item in ingestion_results
    if item["status"] == "SUCCESS"
]

for table_name in successful_tables:
    try:
        print("\n================================")
        print("TABLE:", table_name)
        print("================================")

        df = spark.table(table_name)
        print("SCHEMA:")
        df.printSchema()
        print("SAMPLE DATA:")
        display(df.limit(5))
    except Exception as e:
        print("Verification failed:", str(e))

if not successful_tables:
    print("No successfully ingested tables to verify.")

# ================================================================
# 20. SOURCE-WISE INGESTION SUMMARY
# ================================================================

if summary_df is not None:
    display(
        summary_df.groupBy("source", "status")
        .count()
        .orderBy("source", "status")
    )
else:
    print("No datasets processed.")

# ================================================================
# 21. FINAL STATUS
# ================================================================

print("========================================")
print("MULTI-SOURCE INGESTION FINISHED")
print("========================================")
print("CATALOG:", CATALOG_NAME)
print("SCHEMA:", SCHEMA_NAME)
print("SOURCES: Azure Blob Storage, Azure ADLS Gen2, Azure SQL Server")
print("TARGET FORMAT: DELTA (Unity Catalog managed tables)")
print("WRITE MODE:", WRITE_MODE)
print("NOTE: JSON assumes newline-delimited JSON.")
print("NOTE: Delta directories must end with .delta.")
print("NOTE: Each discovered file is loaded as a separate table.")