# Databricks notebook source
# Silver Config
storage_account = "dlliveraj"
container = "delta-unity-catalog"
storage_key = dbutils.secrets.get(scope="chicago-crime-scope", key="adls-storage-key")

# Set Spark config
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)
bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/stackoverflow"
silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/stackoverflow"

print(f" Bronze path: {bronze_path}")
print(f" Silver path: {silver_path}")

# COMMAND ----------

# Read from Bronze Delta table
#df_bronze = spark.read.format("delta").load(bronze_path)

# New way — UC governed
df_bronze = spark.read.table("stackoverflow_catalog.bronze.survey")

print(f" Row count: {df_bronze.count()}")
print(f"Column count: {len(df_bronze.columns)}")
df_bronze.limit(5).display()

# COMMAND ----------

df_bronze.limit(10).display()

# COMMAND ----------

from pyspark.sql.functions import col

# Select relevant columns for analysis
df_silver = df_bronze.select(
    col("ResponseId"),
    col("MainBranch"),
    col("Age"),
    col("Employment"),
    col("RemoteWork"),
    col("EdLevel"),
    col("YearsCode"),
    col("YearsCodePro"),
    col("DevType"),
    col("Country"),
    col("ConvertedCompYearly").alias("AnnualSalary"),
    col("LanguageHaveWorkedWith"),
    col("LanguageWantToWorkWith"),
    col("DatabaseHaveWorkedWith"),
    col("PlatformHaveWorkedWith"),
    col("AIToolCurrently_Using").alias("AITools"),
    col("ingestion_timestamp"),
    col("source_file"),
    col("layer")
)

print(f"✅ Selected columns: {len(df_silver.columns)}")
df_silver.limit(5).display()

# COMMAND ----------

from pyspark.sql.functions import when, trim, lit

# Clean and cast columns
df_silver = df_silver \
    .withColumn("YearsCode", 
        when(col("YearsCode") == "Less than 1 year", "0")
        .when(col("YearsCode") == "More than 50 years", "51")
        .otherwise(col("YearsCode"))
        .cast("integer")) \
    .withColumn("YearsCodePro",
        when(col("YearsCodePro") == "Less than 1 year", "0")
        .when(col("YearsCodePro") == "More than 50 years", "51")
        .otherwise(col("YearsCodePro"))
        .cast("integer")) \
    .withColumn("AnnualSalary", 
        col("AnnualSalary").cast("double")) \
    .withColumn("MainBranch", trim(col("MainBranch"))) \
    .withColumn("Employment", trim(col("Employment"))) \
    .withColumn("RemoteWork", trim(col("RemoteWork"))) \
    .withColumn("Country", trim(col("Country"))) \
    .withColumn("layer", lit("silver"))

print("✅ Data types cast successfully!")
df_silver.printSchema()

# COMMAND ----------

# Count before cleaning
before_count = df_silver.count()
print(f"Before cleaning: {before_count}")

# Drop duplicates
df_silver = df_silver.dropDuplicates(["ResponseId"])

# Drop rows where critical columns are null
df_silver = df_silver.filter(
    col("ResponseId").isNotNull() &
    col("MainBranch").isNotNull() &
    col("Employment").isNotNull()
)

# Count after cleaning
after_count = df_silver.count()
print(f"✅ After cleaning: {after_count}")
print(f"✅ Rows removed: {before_count - after_count}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Add silver audit columns
df_silver = df_silver \
    .withColumn("silver_timestamp", current_timestamp()) \
    .withColumn("layer", lit("silver"))

print(f"✅ Total columns: {len(df_silver.columns)}")
df_silver.limit(5).display()

# COMMAND ----------

# Write Silver Delta table - initial load
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

print("Silver Delta table written successfully!")

# COMMAND ----------

from delta.tables import DeltaTable

# Load existing Silver Delta table
delta_table = DeltaTable.forPath(spark, silver_path)

# Simulate updated/new records (5 modified rows)
df_updates = df_silver.limit(5) \
    .withColumn("AnnualSalary", col("AnnualSalary") * 1.1) \
    .withColumn("silver_timestamp", current_timestamp())

# Show BEFORE state of those 5 records
print("BEFORE MERGE — Current values in Silver table:")
spark.read.format("delta").load(silver_path) \
    .filter(col("ResponseId").isin(
        [row.ResponseId for row in df_updates.select("ResponseId").collect()]
    )) \
    .select("ResponseId", "AnnualSalary", "silver_timestamp") \
    .display()

# MERGE - Upsert
delta_table.alias("target") \
    .merge(
        df_updates.alias("source"),
        "target.ResponseId = source.ResponseId"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print(" MERGE completed successfully!")

# Show AFTER state of those 5 records
print("AFTER MERGE — Updated values in Silver table:")
spark.read.format("delta").load(silver_path) \
    .filter(col("ResponseId").isin(
        [row.ResponseId for row in df_updates.select("ResponseId").collect()]
    )) \
    .select("ResponseId", "AnnualSalary", "silver_timestamp") \
    .display()

# Final count
df_verify = spark.read.format("delta").load(silver_path)
print(f" Silver row count after MERGE: {df_verify.count()}")

# COMMAND ----------

# Time Travel — Query previous version
print("Current version:")
spark.read.format("delta").load(silver_path) \
    .filter(col("ResponseId") == 833) \
    .select("ResponseId", "AnnualSalary", "silver_timestamp") \
    .display()

print("Version 0 — Before MERGE:")
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(silver_path) \
    .filter(col("ResponseId") == 833) \
    .select("ResponseId", "AnnualSalary", "silver_timestamp") \
    .display()

# COMMAND ----------

# View full Delta table history
from delta.tables import DeltaTable

delta_silver = DeltaTable.forPath(spark, silver_path)
delta_silver.history().select(
    "version",
    "timestamp",
    "operation",
    "operationParameters"
).display()

# COMMAND ----------

# Schema Evolution — Add new column to existing Delta table
from pyspark.sql.functions import when, col

# Read current silver
df_schema_evo = spark.read.format("delta").load(silver_path)

# Add a new column - Experience Category
df_schema_evo = df_schema_evo.withColumn(
    "ExperienceCategory",
    when(col("YearsCodePro") < 2, "Junior")
    .when((col("YearsCodePro") >= 2) & (col("YearsCodePro") < 5), "Mid-Level")
    .when((col("YearsCodePro") >= 5) & (col("YearsCodePro") < 10), "Senior")
    .when(col("YearsCodePro") >= 10, "Expert")
    .otherwise("Unknown")
)

# Write back with schema evolution enabled
df_schema_evo.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

print("Schema Evolution complete — new column added!")

# Verify new schema
df_verify = spark.read.format("delta").load(silver_path)
print(f"Total columns now: {len(df_verify.columns)}")
df_verify.select("ResponseId", "YearsCodePro", "ExperienceCategory").limit(10).display()