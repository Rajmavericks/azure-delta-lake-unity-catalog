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
silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/stackoverflow"
gold_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/stackoverflow"

print(f" Silver path: {silver_path}")
print(f" Gold path: {gold_path}")

# COMMAND ----------

# Read from Silver Delta table
#df_silver = spark.read.format("delta").load(silver_path)

df_silver = spark.read.table("stackoverflow_catalog.silver.survey")

print(f"Row count: {df_silver.count()}")
print(f"Column count: {len(df_silver.columns)}")
df_silver.limit(5).display()

# COMMAND ----------

from pyspark.sql.functions import count, col

# Developer count by country
df_gold_country = df_silver \
    .groupBy("Country") \
    .agg(count("ResponseId").alias("DeveloperCount")) \
    .orderBy(col("DeveloperCount").desc())

print(f"Total countries: {df_gold_country.count()}")
df_gold_country.limit(10).display()

# COMMAND ----------

from pyspark.sql.functions import avg, round

# Average salary by experience category
df_gold_salary = df_silver \
    .filter(col("AnnualSalary").isNotNull()) \
    .groupBy("ExperienceCategory") \
    .agg(
        round(avg("AnnualSalary"), 2).alias("AvgAnnualSalary"),
        count("ResponseId").alias("DeveloperCount")
    ) \
    .orderBy(col("AvgAnnualSalary").desc())

print(f"Experience categories: {df_gold_salary.count()}")
df_gold_salary.display()

# COMMAND ----------

from pyspark.sql.functions import avg, round

# Average salary by experience category
df_gold_salary = df_silver \
    .filter(col("AnnualSalary").isNotNull()) \
    .groupBy("ExperienceCategory") \
    .agg(
        round(avg("AnnualSalary"), 2).alias("AvgAnnualSalary"),
        count("ResponseId").alias("DeveloperCount")
    ) \
    .orderBy(col("AvgAnnualSalary").desc())

print(f"Experience categories: {df_gold_salary.count()}")
df_gold_salary.display()

# COMMAND ----------

from pyspark.sql.functions import split, explode, trim

# Explode languages (semicolon separated)
df_languages = df_silver \
    .filter(col("LanguageHaveWorkedWith").isNotNull()) \
    .select(
        explode(
            split(col("LanguageHaveWorkedWith"), ";")
        ).alias("Language")
    ) \
    .withColumn("Language", trim(col("Language"))) \
    .groupBy("Language") \
    .agg(count("*").alias("DeveloperCount")) \
    .orderBy(col("DeveloperCount").desc())

print(f"Total languages found: {df_languages.count()}")
df_languages.limit(10).display()

# COMMAND ----------

# Remote work distribution
df_gold_remote = df_silver \
    .filter(col("RemoteWork").isNotNull()) \
    .groupBy("RemoteWork") \
    .agg(
        count("ResponseId").alias("DeveloperCount"),
        round(
            count("ResponseId") * 100.0 / df_silver.count(), 2
        ).alias("Percentage")
    ) \
    .orderBy(col("DeveloperCount").desc())

print(f"Remote work categories: {df_gold_remote.count()}")
df_gold_remote.display()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Add audit columns to each aggregation
df_gold_country_final = df_gold_country \
    .withColumn("aggregation_type", lit("developer_count_by_country")) \
    .withColumn("gold_timestamp", current_timestamp())

df_gold_salary_final = df_gold_salary \
    .withColumn("aggregation_type", lit("avg_salary_by_experience")) \
    .withColumn("gold_timestamp", current_timestamp())

df_gold_languages_final = df_languages \
    .withColumn("aggregation_type", lit("top_languages")) \
    .withColumn("gold_timestamp", current_timestamp())

df_gold_remote_final = df_gold_remote \
    .withColumn("aggregation_type", lit("remote_work_distribution")) \
    .withColumn("gold_timestamp", current_timestamp())

# Write each as separate Gold Delta table
df_gold_country_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true")\
    .save(f"{gold_path}/country_stats")
df_gold_salary_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/salary_stats")
df_gold_languages_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/language_stats")
df_gold_remote_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/remote_stats")

print("Gold Delta tables written successfully!")
print(f"country_stats    : {df_gold_country_final.count()} rows")
print(f"salary_stats     : {df_gold_salary_final.count()} rows")
print(f"language_stats   : {df_gold_languages_final.count()} rows")
print(f"remote_stats     : {df_gold_remote_final.count()} rows")

# COMMAND ----------

# Verify all Gold Delta tables
tables = ["country_stats", "salary_stats", "language_stats", "remote_stats"]

for table in tables:
    df = spark.read.format("delta").load(f"{gold_path}/{table}")
    print(f"{table}: {df.count()} rows | {len(df.columns)} columns")
    df.limit(3).display()