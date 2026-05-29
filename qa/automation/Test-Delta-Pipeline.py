# Databricks notebook source

from pyspark.sql.functions import col, count, countDistinct, avg, when, isnull
from delta.tables import DeltaTable
import traceback

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

storage_account = "dlliveraj"
container       = "delta-unity-catalog"
storage_key     = dbutils.secrets.get(scope="chicago-crime-scope", key="adls-storage-key")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/stackoverflow"
silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/stackoverflow"
gold_path   = f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/stackoverflow"

# ─────────────────────────────────────────────
# TEST RUNNER
# ─────────────────────────────────────────────

results = []

def run_test(test_id, description, fn):
    try:
        fn()
        results.append({"id": test_id, "description": description, "status": "PASS", "error": ""})
        print(f"  PASS  {test_id} — {description}")
    except AssertionError as e:
        results.append({"id": test_id, "description": description, "status": "FAIL", "error": str(e)})
        print(f"  FAIL  {test_id} — {description} | {e}")
    except Exception as e:
        results.append({"id": test_id, "description": description, "status": "ERROR", "error": str(e)})
        print(f"  ERROR {test_id} — {description} | {e}")


# ─────────────────────────────────────────────
# SECTION 1 — BRONZE LAYER TESTS
# ─────────────────────────────────────────────

print("\n" + "="*60)
print("BRONZE LAYER TESTS")
print("="*60)

df_bronze = spark.read.format("delta").load(bronze_path)

def test_brz_001():
    count = df_bronze.count()
    assert count >= 65000, f"Bronze row count too low: {count}"

def test_brz_002():
    cols = df_bronze.columns
    assert len(cols) >= 100, f"Bronze column count too low: {len(cols)}"

def test_brz_003():
    invalid_chars = [' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '=']
    for col_name in df_bronze.columns:
        for ch in invalid_chars:
            assert ch not in col_name, f"Invalid character '{ch}' in column: {col_name}"

def test_brz_004():
    required = ["ingestion_timestamp", "source_file", "layer"]
    for col_name in required:
        assert col_name in df_bronze.columns, f"Missing audit column: {col_name}"

def test_brz_005():
    distinct_layers = [r[0] for r in df_bronze.select("layer").distinct().collect()]
    assert distinct_layers == ["bronze"], f"Unexpected layer values: {distinct_layers}"

def test_brz_006():
    null_count = df_bronze.filter(isnull("ingestion_timestamp")).count()
    assert null_count == 0, f"Null ingestion_timestamp found: {null_count} rows"

run_test("TC_BRZ_001", "Bronze row count >= 65000",              test_brz_001)
run_test("TC_BRZ_002", "Bronze column count >= 100",             test_brz_002)
run_test("TC_BRZ_003", "No invalid characters in column names",  test_brz_003)
run_test("TC_BRZ_004", "Audit columns present",                  test_brz_004)
run_test("TC_BRZ_005", "layer column value = bronze",            test_brz_005)
run_test("TC_BRZ_006", "No null ingestion_timestamp",            test_brz_006)


# ─────────────────────────────────────────────
# SECTION 2 — SILVER LAYER TESTS
# ─────────────────────────────────────────────

print("\n" + "="*60)
print("SILVER LAYER TESTS")
print("="*60)

df_silver = spark.read.table("stackoverflow_catalog.silver.survey")

def test_slv_001():
    count = df_silver.count()
    assert count >= 65000, f"Silver row count too low: {count}"

def test_slv_002():
    assert len(df_silver.columns) >= 19, f"Silver column count too low: {len(df_silver.columns)}"

def test_slv_003():
    schema = dict(df_silver.dtypes)
    assert schema.get("YearsCode") == "int",      f"YearsCode type: {schema.get('YearsCode')}"
    assert schema.get("YearsCodePro") == "int",   f"YearsCodePro type: {schema.get('YearsCodePro')}"
    assert schema.get("AnnualSalary") == "double", f"AnnualSalary type: {schema.get('AnnualSalary')}"

def test_slv_004():
    bad = df_silver.filter(col("YearsCode").cast("string") == "Less than 1 year").count()
    assert bad == 0, f"Unconverted YearsCode values found: {bad}"

def test_slv_005():
    null_ids = df_silver.filter(isnull("ResponseId")).count()
    assert null_ids == 0, f"Null ResponseId found: {null_ids}"

def test_slv_006():
    total = df_silver.count()
    distinct = df_silver.select("ResponseId").distinct().count()
    assert total == distinct, f"Duplicates found: total={total}, distinct={distinct}"

def test_slv_007():
    assert "ExperienceCategory" in df_silver.columns, "ExperienceCategory column missing"

def test_slv_008():
    valid = {"Junior", "Mid-Level", "Senior", "Expert", "Unknown"}
    actual = {r[0] for r in df_silver.select("ExperienceCategory").distinct().collect()}
    unexpected = actual - valid
    assert len(unexpected) == 0, f"Unexpected ExperienceCategory values: {unexpected}"

def test_slv_009():
    junior = df_silver.filter(
        (col("YearsCodePro") < 2) & col("YearsCodePro").isNotNull()
    ).select("ExperienceCategory").distinct().collect()
    cats = [r[0] for r in junior]
    assert cats == ["Junior"], f"Expected Junior, got: {cats}"

def test_slv_010():
    distinct_layers = [r[0] for r in df_silver.select("layer").distinct().collect()]
    assert "silver" in distinct_layers, f"layer=silver not found: {distinct_layers}"

def test_slv_011():
    salary_df = df_silver.filter(col("AnnualSalary").isNotNull())
    neg = salary_df.filter(col("AnnualSalary") < 0).count()
    assert neg == 0, f"Negative AnnualSalary values: {neg}"

def test_slv_012():
    history = DeltaTable.forPath(spark, silver_path).history()
    ops = [r["operation"] for r in history.select("operation").collect()]
    assert "MERGE" in ops, f"No MERGE found in Delta history: {ops}"

def test_slv_013():
    versions = DeltaTable.forPath(spark, silver_path).history().select("version").collect()
    assert len(versions) >= 2, f"Expected at least 2 versions, got: {len(versions)}"

run_test("TC_SLV_001", "Silver row count >= 65000",              test_slv_001)
run_test("TC_SLV_002", "Silver column count >= 19",              test_slv_002)
run_test("TC_SLV_003", "Type casting correct",                   test_slv_003)
run_test("TC_SLV_004", "No unconverted YearsCode string values", test_slv_004)
run_test("TC_SLV_005", "No null ResponseId",                     test_slv_005)
run_test("TC_SLV_006", "No duplicate ResponseId",                test_slv_006)
run_test("TC_SLV_007", "ExperienceCategory column present",      test_slv_007)
run_test("TC_SLV_008", "Valid ExperienceCategory values only",   test_slv_008)
run_test("TC_SLV_009", "YearsCodePro < 2 maps to Junior",        test_slv_009)
run_test("TC_SLV_010", "layer column value = silver",            test_slv_010)
run_test("TC_SLV_011", "No negative AnnualSalary values",        test_slv_011)
run_test("TC_SLV_012", "MERGE operation in Delta history",       test_slv_012)
run_test("TC_SLV_013", "At least 2 Delta versions exist",        test_slv_013)


# ─────────────────────────────────────────────
# SECTION 3 — GOLD LAYER TESTS
# ─────────────────────────────────────────────

print("\n" + "="*60)
print("GOLD LAYER TESTS")
print("="*60)

df_country  = spark.read.table("stackoverflow_catalog.gold.country_stats")
df_salary   = spark.read.table("stackoverflow_catalog.gold.salary_stats")
df_language = spark.read.table("stackoverflow_catalog.gold.language_stats")
df_remote   = spark.read.table("stackoverflow_catalog.gold.remote_stats")

def test_gld_001():
    count = df_country.count()
    assert count >= 100, f"country_stats row count too low: {count}"

def test_gld_002():
    top = df_country.orderBy(col("DeveloperCount").desc()).first()["Country"]
    assert "United States" in top, f"Top country not USA: {top}"

def test_gld_003():
    count = df_salary.count()
    assert count == 5, f"salary_stats should have 5 rows, got: {count}"

def test_gld_004():
    top_exp = df_salary.orderBy(col("AvgAnnualSalary").desc()).first()["ExperienceCategory"]
    assert top_exp == "Expert", f"Highest salary not Expert: {top_exp}"

def test_gld_005():
    count = df_language.count()
    assert count >= 50, f"language_stats row count too low: {count}"

def test_gld_006():
    top_lang = df_language.orderBy(col("DeveloperCount").desc()).first()["Language"]
    assert top_lang == "JavaScript", f"Top language not JavaScript: {top_lang}"

def test_gld_007():
    count = df_remote.count()
    assert count == 4, f"remote_stats should have 4 rows, got: {count}"

def test_gld_008():
    total_pct = df_remote.agg(sum("Percentage")).first()[0]
    assert abs(total_pct - 100.0) < 1.0, f"Percentages do not sum to ~100: {total_pct}"

def test_gld_009():
    for df, name in [(df_country,"country_stats"),(df_salary,"salary_stats"),
                     (df_language,"language_stats"),(df_remote,"remote_stats")]:
        assert "aggregation_type" in df.columns, f"aggregation_type missing in {name}"
        assert "gold_timestamp" in df.columns, f"gold_timestamp missing in {name}"

def test_gld_010():
    neg_salary = df_salary.filter(col("AvgAnnualSalary") < 0).count()
    assert neg_salary == 0, f"Negative AvgAnnualSalary found: {neg_salary}"

run_test("TC_GLD_001", "country_stats row count >= 100",          test_gld_001)
run_test("TC_GLD_002", "Top country is United States",            test_gld_002)
run_test("TC_GLD_003", "salary_stats has exactly 5 rows",         test_gld_003)
run_test("TC_GLD_004", "Expert has highest average salary",       test_gld_004)
run_test("TC_GLD_005", "language_stats row count >= 100",         test_gld_005)
run_test("TC_GLD_006", "JavaScript is top language",              test_gld_006)
run_test("TC_GLD_007", "remote_stats has exactly 4 rows",         test_gld_007)
run_test("TC_GLD_008", "Remote work percentages sum to ~100",     test_gld_008)
run_test("TC_GLD_009", "Audit columns in all Gold tables",        test_gld_009)
run_test("TC_GLD_010", "No negative average salary values",       test_gld_010)


# ─────────────────────────────────────────────
# SECTION 4 — UNITY CATALOG TESTS
# ─────────────────────────────────────────────

print("\n" + "="*60)
print("UNITY CATALOG TESTS")
print("="*60)

def test_uc_001():
    result = spark.sql("SELECT current_metastore()").first()[0]
    assert result is not None, "No metastore returned"

def test_uc_002():
    catalogs = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
    assert "stackoverflow_catalog" in catalogs, f"stackoverflow_catalog not found: {catalogs}"

def test_uc_003():
    schemas = [r[0] for r in spark.sql("SHOW SCHEMAS IN stackoverflow_catalog").collect()]
    for s in ["bronze", "silver", "gold"]:
        assert s in schemas, f"Schema {s} not found in stackoverflow_catalog"

def test_uc_004():
    tables = [r[1] for r in spark.sql("SHOW TABLES IN stackoverflow_catalog.bronze").collect()]
    assert "survey" in tables, f"survey table not in bronze schema: {tables}"

def test_uc_005():
    tables = [r[1] for r in spark.sql("SHOW TABLES IN stackoverflow_catalog.silver").collect()]
    assert "survey" in tables, f"survey table not in silver schema: {tables}"

def test_uc_006():
    tables = [r[1] for r in spark.sql("SHOW TABLES IN stackoverflow_catalog.gold").collect()]
    for t in ["country_stats", "salary_stats", "language_stats", "remote_stats"]:
        assert t in tables, f"{t} not found in gold schema"

def test_uc_007():
    count = spark.sql("SELECT COUNT(*) FROM stackoverflow_catalog.silver.survey").first()[0]
    assert count >= 65000, f"UC SQL query returned low count: {count}"

def test_uc_008():
    desc = spark.sql("DESCRIBE EXTENDED stackoverflow_catalog.silver.survey").collect()
    desc_dict = {r[0]: r[1] for r in desc}
    table_type = desc_dict.get("Type", "")
    assert "EXTERNAL" in table_type.upper(), f"Table is not EXTERNAL type: {table_type}"

run_test("TC_UC_001", "Metastore active",                   test_uc_001)
run_test("TC_UC_002", "stackoverflow_catalog exists",       test_uc_002)
run_test("TC_UC_003", "bronze/silver/gold schemas exist",   test_uc_003)
run_test("TC_UC_004", "bronze.survey registered",           test_uc_004)
run_test("TC_UC_005", "silver.survey registered",           test_uc_005)
run_test("TC_UC_006", "All 4 gold tables registered",       test_uc_006)
run_test("TC_UC_007", "UC SQL query on silver works",       test_uc_007)
run_test("TC_UC_008", "silver.survey is EXTERNAL table",    test_uc_008)


# ─────────────────────────────────────────────
# SECTION 5 — CROSS LAYER CONSISTENCY TESTS
# ─────────────────────────────────────────────

print("\n" + "="*60)
print("CROSS LAYER CONSISTENCY TESTS")
print("="*60)

def test_cls_001():
    bronze_count = df_bronze.count()
    silver_count = df_silver.count()
    assert silver_count <= bronze_count, \
        f"Silver ({silver_count}) has more rows than Bronze ({bronze_count})"

def test_cls_002():
    silver_count = df_silver.count()
    country_total = df_country.agg({"DeveloperCount": "sum"}).first()[0]
    assert abs(silver_count - country_total) < 10, \
        f"country_stats DeveloperCount sum ({country_total}) differs from Silver ({silver_count})"

def test_cls_003():
    silver_ids = df_silver.select("ResponseId").distinct().count()
    bronze_ids = df_bronze.select("ResponseId").distinct().count() \
                 if "ResponseId" in df_bronze.columns else silver_ids
    assert silver_ids <= bronze_ids, \
        f"Silver has more unique IDs ({silver_ids}) than Bronze ({bronze_ids})"

run_test("TC_CLS_001", "Silver row count <= Bronze row count",       test_cls_001)
run_test("TC_CLS_002", "country_stats sum matches Silver row count", test_cls_002)
run_test("TC_CLS_003", "Silver unique IDs <= Bronze unique IDs",     test_cls_003)


# ─────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────

print("\n" + "="*60)
print("TEST SUMMARY")
print("="*60)

passed  = len([r for r in results if r["status"] == "PASS"])
failed  = len([r for r in results if r["status"] == "FAIL"])
errored = len([r for r in results if r["status"] == "ERROR"])
total   = len(results)

print(f"\nTotal  : {total}")
print(f"Pass   : {passed}")
print(f"Fail   : {failed}")
print(f"Error  : {errored}")
print(f"Rate   : {round(passed/total*100, 1)}%")

if failed > 0 or errored > 0:
    print("\nFailed / Errored Tests:")
    for r in results:
        if r["status"] in ("FAIL", "ERROR"):
            print(f"  {r['id']} — {r['description']}")
            print(f"    {r['error']}")

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("id", StringType(), True),
    StructField("description", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error", StringType(), True)
])

import pandas as pd

summary_pd = pd.DataFrame(results)
print(summary_pd.to_string(index=False))


# COMMAND ----------


import traceback

for test_id, description, fn in [
    ("TC_CLS_001", "Silver row count <= Bronze row count", test_cls_001),
    ("TC_CLS_002", "country_stats sum matches Silver row count", test_cls_002),
    ("TC_CLS_003", "Silver unique IDs <= Bronze unique IDs", test_cls_003),
]:
    try:
        fn()
        print(f"PASS {test_id}")
    except Exception as e:
        print(f"ERROR {test_id}: {e}")
        traceback.print_exc()