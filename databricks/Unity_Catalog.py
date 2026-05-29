# Databricks notebook source
# Step 1 — Create Catalog
spark.sql("CREATE CATALOG IF NOT EXISTS stackoverflow_catalog")
print("Catalog created.")

# Step 2 — Create Schemas for each layer
spark.sql("CREATE SCHEMA IF NOT EXISTS stackoverflow_catalog.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS stackoverflow_catalog.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS stackoverflow_catalog.gold")
print("Schemas created.")

# Verify
spark.sql("SHOW SCHEMAS IN stackoverflow_catalog").display()

# COMMAND ----------

spark.sql("GRANT CREATE EXTERNAL LOCATION ON METASTORE TO `rajinigcp@gmail.com`")

# COMMAND ----------

# Register Bronze table
spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow_catalog.bronze.survey
    USING DELTA
    LOCATION 'abfss://delta-unity-catalog@dlliveraj.dfs.core.windows.net/bronze/stackoverflow'
""")
print("Bronze table registered.")

# Register Silver table
spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow_catalog.silver.survey
    USING DELTA
    LOCATION 'abfss://delta-unity-catalog@dlliveraj.dfs.core.windows.net/silver/stackoverflow'
""")
print("Silver table registered.")

# Register Gold tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow_catalog.gold.country_stats
    USING DELTA
    LOCATION 'abfss://delta-unity-catalog@dlliveraj.dfs.core.windows.net/gold/stackoverflow/country_stats'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow_catalog.gold.salary_stats
    USING DELTA
    LOCATION 'abfss://delta-unity-catalog@dlliveraj.dfs.core.windows.net/gold/stackoverflow/salary_stats'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow_catalog.gold.language_stats
    USING DELTA
    LOCATION 'abfss://delta-unity-catalog@dlliveraj.dfs.core.windows.net/gold/stackoverflow/language_stats'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS stackoverflow_catalog.gold.remote_stats
    USING DELTA
    LOCATION 'abfss://delta-unity-catalog@dlliveraj.dfs.core.windows.net/gold/stackoverflow/remote_stats'
""")
print("Gold tables registered.")

# COMMAND ----------

# Verify all tables registered in Unity Catalog
spark.sql("SHOW TABLES IN stackoverflow_catalog.bronze").display()
spark.sql("SHOW TABLES IN stackoverflow_catalog.silver").display()
spark.sql("SHOW TABLES IN stackoverflow_catalog.gold").display()

# COMMAND ----------

# Verify UC table reads work correctly
df_test = spark.read.table("stackoverflow_catalog.silver.survey")
print(f"Silver via UC — Row count: {df_test.count()}")
print(f"Silver via UC — Column count: {len(df_test.columns)}")

df_gold_test = spark.read.table("stackoverflow_catalog.gold.salary_stats")
print(f"Gold salary_stats via UC — Row count: {df_gold_test.count()}")

print("Unity Catalog reads working correctly!")

# COMMAND ----------

df = spark.read.table("stackoverflow_catalog.silver.survey")
print(f"Row count: {df.count()}")