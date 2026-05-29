# Databricks notebook source
# Config
storage_account = "dlliveraj"
container = "delta-unity-catalog"
storage_key = dbutils.secrets.get(scope="chicago-crime-scope", key="adls-storage-key")

# Set Spark config
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Paths
raw_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/raw/delta-unity-catalog.csv"
bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/stackoverflow"

print(f"Raw path: {raw_path}")
print(f"Bronze path: {bronze_path}")

# COMMAND ----------

# Read raw CSV
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load(raw_path)

print(f"✅ Row count: {df_raw.count()}")
print(f"✅ Column count: {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

import re

# Clean column names - remove invalid characters
def clean_column_name(col_name):
    # Replace spaces and special chars with underscore
    col_name = re.sub(r'[ ,;{}()\n\t=]', '_', col_name)
    # Remove consecutive underscores
    col_name = re.sub(r'_+', '_', col_name)
    # Strip leading/trailing underscores
    col_name = col_name.strip('_')
    return col_name

# Apply to all columns
cleaned_columns = [clean_column_name(c) for c in df_raw.columns]
df_raw = df_raw.toDF(*cleaned_columns)

print("✅ Column names cleaned!")
print(f"Sample cleaned columns: {cleaned_columns[:10]}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Add audit columns
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("survey_results_public.csv")) \
    .withColumn("layer", lit("bronze"))

print(f"✅ Bronze columns: {len(df_bronze.columns)}")
df_bronze.limit(5).display()

# COMMAND ----------

# Write to bronze as Delta
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_path)

print("✅ Bronze Delta table written successfully!")

# COMMAND ----------

# Read back and verify
df_verify = spark.read.format("delta").load(bronze_path)
print(f"✅ Bronze Delta row count: {df_verify.count()}")
df_verify.limit(5).display()