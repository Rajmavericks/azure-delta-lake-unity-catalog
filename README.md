# Delta Lake Architecture with Unity Catalog on Azure Databricks

## Project Overview

This project implements a production-grade Delta Lake data pipeline on Azure Databricks following the Medallion Architecture pattern (Bronze, Silver, Gold). It ingests Stack Overflow Developer Survey 2024 data from a local machine into Azure Data Lake Storage Gen2, processes it through three transformation layers, and governs all data assets through Unity Catalog.

The pipeline is fully orchestrated via Azure Data Factory and demonstrates enterprise-grade data engineering patterns including Delta Lake ACID transactions, SCD Type 1 MERGE operations, Time Travel, Schema Evolution, and Unity Catalog data governance.

---

## Architecture

```
Local Machine
    |
    | Self-Hosted Integration Runtime
    |
Azure Data Factory (PL_StackOverflow_Master)
    |
    |-- CopyLocalToADLSSSH
    |       Local CSV --> ADLS Gen2 raw/
    |
    |-- ACT_Bronze_StackOverflow (on Succeeded)
    |       Raw CSV --> Bronze Delta Table
    |       Audit columns added (ingestion_timestamp, source_file, layer)
    |
    |-- ACT_Silver_StackOverflow (on Succeeded)
    |       Bronze --> Silver Delta Table
    |       Column selection, type casting, null handling
    |       SCD Type 1 MERGE, Time Travel, Schema Evolution
    |
    |-- ACT_Gold_StackOverflow (on Succeeded)
            Silver --> Gold Delta Tables (4 aggregations)
            Registered in Unity Catalog

Unity Catalog (stackoverflow-metastore)
    |
    |-- stackoverflow_catalog
            |-- bronze.survey
            |-- silver.survey
            |-- gold.country_stats
            |-- gold.salary_stats
            |-- gold.language_stats
            |-- gold.remote_stats
```

---

## Technology Stack

| Component | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Lake Storage | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks (Premium) |
| Orchestration | Azure Data Factory |
| Data Format | Delta Lake (Parquet + Transaction Log) |
| Data Governance | Unity Catalog |
| Language | PySpark (Python) |
| Version Control | GitHub |
| Authentication | Azure Key Vault + Databricks Secrets |
| Integration Runtime | Self-Hosted IR (local file ingestion) |

---

## Dataset

| Field | Detail |
|---|---|
| Source | Stack Overflow Annual Developer Survey 2024 |
| URL | https://survey.stackoverflow.co/2024 |
| Format | CSV |
| Size | ~158 MB |
| Rows | 65,437 survey responses |
| Columns | 117 raw columns (reduced to 19 in Silver layer) |
| Domain | Developer tools, salaries, languages, remote work trends |

---

## Repository Structure

```
azure-delta-lake-unity-catalog/
|
|-- README.md
|
|-- adf/
|   |-- pipeline/
|   |   |-- PL_StackOverflow_Master.json
|   |   |-- PL_LocalToADLSIngestion.json
|   |-- linkedService/
|   |   |-- LS_Databricks_DeltaUnity.json
|   |   |-- LS_ADLS_DeltaUnity.json
|   |   |-- LS_LocalFileSystem.json
|   |-- dataset/
|       |-- DS_Local_ADLS_SSH.json
|       |-- DS_SOF_Sink_ADLS_CSV.json
|
|-- databricks/
|   |-- bronze/
|   |   |-- NB_Bronze_StackOverflow.py
|   |-- silver/
|   |   |-- NB_Silver_StackOverflow.py
|   |-- gold/
|       |-- NB_Gold_StackOverflow.py
|
|-- unity_catalog/
|   |-- setup/
|       |-- UC_Setup_Commands.sql
|
|-- qa/
|   |-- test_cases/
|   |-- automation/
|   |-- ci_cd/
|
|-- docs/
|   |-- architecture/
|   |-- implementation_guide/
|       |-- Unity_Catalog_Setup_Guide.docx
|
|-- powerbi/
```

---

## Pipeline Details

### ADF Master Pipeline — PL_StackOverflow_Master

| Activity | Type | Depends On | Description |
|---|---|---|---|
| CopyLocalToADLSSSH | Copy Activity | None | Copies CSV from local machine to ADLS raw/ via Self-Hosted IR |
| ACT_Bronze_StackOverflow | Databricks Notebook | CopyLocalToADLSSSH Succeeded | Ingests raw CSV and writes Bronze Delta table |
| ACT_Silver_StackOverflow | Databricks Notebook | ACT_Bronze Succeeded | Transforms Bronze and writes Silver Delta table |
| ACT_Gold_StackOverflow | Databricks Notebook | ACT_Silver Succeeded | Aggregates Silver and writes 4 Gold Delta tables |

---

### Bronze Layer — NB_Bronze_StackOverflow

Reads raw CSV from ADLS and writes as a Delta table with audit columns.

```python
# Read raw CSV
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load(raw_path)

# Add audit columns
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("survey_results_public.csv")) \
    .withColumn("layer", lit("bronze"))

# Write as Delta
df_bronze.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_path)
```

**Output:** 65,437 rows | 120 columns (117 raw + 3 audit)

---

### Silver Layer — NB_Silver_StackOverflow

Reads Bronze Delta table via Unity Catalog, applies transformations, and demonstrates key Delta Lake features.

**Transformations applied:**
- Column selection — 19 business-relevant columns from 117
- Data type casting — YearsCode, YearsCodePro to integer, AnnualSalary to double
- Special value handling — "Less than 1 year" mapped to 0, "More than 50 years" mapped to 51
- Null and duplicate removal on critical columns
- Schema Evolution — ExperienceCategory derived column added

**Delta Lake features demonstrated:**

SCD Type 1 MERGE:
```python
delta_table.alias("target") \
    .merge(df_updates.alias("source"),
           "target.ResponseId = source.ResponseId") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

Time Travel:
```python
# Query historical version
spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(silver_path) \
    .filter(col("ResponseId") == 833) \
    .display()
```

Delta History:
```python
DeltaTable.forPath(spark, silver_path).history().display()
```

**Output:** 65,437 rows | 21 columns

---

### Gold Layer — NB_Gold_StackOverflow

Reads Silver via Unity Catalog and produces 4 business aggregation tables.

| Gold Table | Description | Rows |
|---|---|---|
| country_stats | Developer count by country | 186 |
| salary_stats | Average annual salary by experience category | 5 |
| language_stats | Programming language usage frequency | 327,925 |
| remote_stats | Remote work distribution with percentages | 4 |

**Key insight from salary_stats:**

| Experience | Avg Annual Salary | Developer Count |
|---|---|---|
| Expert (10+ years) | $116,245 | 9,898 |
| Senior (5-10 years) | $82,247 | 6,513 |
| Mid-Level (2-5 years) | $50,874 | 5,304 |
| Junior (0-2 years) | $34,228 | 1,630 |

**Key insight from remote_stats:**

| Work Mode | Developers | Percentage |
|---|---|---|
| Hybrid | 23,015 | 35.17% |
| Remote | 20,831 | 31.83% |
| In-person | 10,960 | 16.75% |

---

## Unity Catalog Setup

Unity Catalog governs all data assets in this project. Tables are registered as external tables — Unity Catalog manages metadata and access control while data files remain in ADLS.

### Namespace Structure

```
stackoverflow-metastore (Central India)
    |
    |-- stackoverflow_catalog
            |-- bronze
            |   |-- survey (external table)
            |-- silver
            |   |-- survey (external table)
            |-- gold
                |-- country_stats (external table)
                |-- salary_stats (external table)
                |-- language_stats (external table)
                |-- remote_stats (external table)
```

### Key Setup Commands

```sql
-- Grant permissions
GRANT CREATE CATALOG ON METASTORE TO `your-email@gmail.com`;
GRANT CREATE EXTERNAL LOCATION ON METASTORE TO `your-email@gmail.com`;
GRANT CREATE STORAGE CREDENTIAL ON METASTORE TO `your-email@gmail.com`;

-- Create catalog and schemas
CREATE CATALOG IF NOT EXISTS stackoverflow_catalog;
CREATE SCHEMA IF NOT EXISTS stackoverflow_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS stackoverflow_catalog.silver;
CREATE SCHEMA IF NOT EXISTS stackoverflow_catalog.gold;

-- Register external tables
CREATE TABLE IF NOT EXISTS stackoverflow_catalog.silver.survey
USING DELTA
LOCATION 'abfss://delta-unity-catalog@<storage>.dfs.core.windows.net/silver/stackoverflow';
```

For complete setup instructions refer to: `docs/implementation_guide/Unity_Catalog_Setup_Guide.docx`

---

## ADLS Gen2 Container Structure

```
delta-unity-catalog/
|-- raw/
|   |-- survey_results_public.csv
|-- bronze/
|   |-- stackoverflow/
|       |-- _delta_log/
|       |-- part-*.parquet
|-- silver/
|   |-- stackoverflow/
|       |-- _delta_log/
|       |-- part-*.parquet
|-- gold/
    |-- stackoverflow/
        |-- country_stats/
        |-- salary_stats/
        |-- language_stats/
        |-- remote_stats/
```

---

## How to Run

### Prerequisites

- Azure subscription with Contributor access
- Azure Databricks workspace (Premium tier)
- Azure Data Lake Storage Gen2
- Azure Data Factory
- Self-Hosted Integration Runtime installed on local machine
- Databricks cluster with Single user access mode and Runtime 15.4 LTS

### Setup Steps

1. Clone this repository
```bash
git clone https://github.com/your-username/azure-delta-lake-unity-catalog.git
cd azure-delta-lake-unity-catalog
```

2. Configure Azure resources following `docs/implementation_guide/Unity_Catalog_Setup_Guide.docx`

3. Update storage account name and secret scope in each notebook:
```python
storage_account = "your_storage_account"
storage_key = dbutils.secrets.get(scope="your-scope", key="your-key")
```

4. Import ADF pipeline JSON files from `adf/pipeline/` into your Data Factory

5. Configure Self-Hosted IR and local file path in `LS_LocalFileSystem` linked service

6. Trigger pipeline:
```
ADF Studio → PL_StackOverflow_Master → Add trigger → Trigger now
```

---

## Key Features Demonstrated

| Feature | Layer | Description |
|---|---|---|
| Delta Lake ACID Transactions | All layers | Full atomicity on all writes |
| SCD Type 1 MERGE | Silver | Upsert with before/after comparison |
| Time Travel | Silver | Query historical versions via versionAsOf |
| Schema Evolution | Silver | Adding new columns without breaking pipeline |
| Delta Transaction Log | Silver | Full audit history of all operations |
| External Tables | Unity Catalog | Data ownership in ADLS independent of UC |
| Data Governance | Unity Catalog | Role-based access control on all tables |
| Data Lineage | Unity Catalog | Full upstream/downstream tracking |
| Self-Hosted IR | ADF | Local file ingestion to cloud storage |
| Secret Management | Databricks | Azure Key Vault integration via secret scope |
| Pipeline Orchestration | ADF | Dependency-chained notebook execution |

---

## Pipeline Performance

| Metric | Value |
|---|---|
| Data ingestion throughput | 53,175 KB/s via Self-Hosted IR |
| Copy duration (158MB CSV) | 3 seconds |
| Bronze notebook duration | ~4-5 minutes |
| Silver notebook duration | ~3-5 minutes |
| Gold notebook duration | ~2-4 minutes |
| End to end pipeline duration | ~10-15 minutes |

---

## Author

**Rajkumar Rajendran**
Quality Engineering Manager | QA Automation Architect | Data Engineering
Coimbatore, Tamil Nadu, India

Part of a multi-project Data Engineering portfolio covering:
- Project 1 — Azure Data Factory fundamentals
- Project 2 — Chicago Crime Data Pipeline (ADF + Databricks)
- Project 3 — Delta Lake Architecture + Unity Catalog (this project)

---

## License

This project is licensed under the MIT License.
