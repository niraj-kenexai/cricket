# 🚀 Complete Step-by-Step Master Guide: Building Your Data Pipeline from Scratch

This comprehensive guide provides an end-to-end, production-grade tutorial on how to build, configure, and operate a modern data engineering pipeline using **Apache Airflow (Astronomer)**, **Snowflake Data Cloud**, and **dbt (Data Build Tool with Astronomer Cosmos)** following the **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**.

---

## 📋 Table of Contents
1. [Architecture & Blueprint Overview](#1-architecture--blueprint-overview)
2. [Pre-Configured Credentials & Environment](#2-pre-configured-credentials--environment)
3. [Phase 1: Snowflake Environment Setup (SQL Worksheet)](#3-phase-1-snowflake-environment-setup)
4. [Phase 2: Launching Your Local Airflow Environment](#4-phase-2-launching-your-local-airflow-environment)
5. [Phase 3: Defining Your Raw Table & Ingestion DAG (Bronze Layer)](#5-phase-3-defining-your-raw-table--ingestion-dag)
6. [Phase 4: Building Your Dimensional & Fact Models (Silver Layer)](#6-phase-4-building-your-dimensional--fact-models-silver-layer)
7. [Phase 5: Building Business KPIs & Analytics (Gold Layer)](#7-phase-5-building-business-kpis--analytics-gold-layer)
8. [Phase 6: End-to-End Orchestration with Cosmos & Airflow](#8-phase-6-end-to-end-orchestration-with-cosmos--airflow)
9. [Phase 7: Critical Snowflake SQL Functions & Syntax Cheat Sheet](#9-phase-7-critical-snowflake-sql-functions--syntax-cheat-sheet)
10. [Phase 8: Verification & Production Troubleshooting Runbook](#10-phase-8-verification--production-troubleshooting-runbook)

---

## 1. Architecture & Blueprint Overview

The pipeline follows the industry-standard **Medallion Data Architecture**:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                 1. BRONZE LAYER (RAW DATA)                             │
│                                                                                        │
│  [ Incoming CSV Files ] ──► [ Airflow PythonSensor ] ──► [ Airflow PythonOperator ]    │
│                               (Queries FILE_AUDIT)        │                            │
│                                                           ├──► Snowflake PUT (@Stage)  │
│                                                           └──► Snowflake COPY INTO     │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              2. SILVER LAYER (CONFORMED & CLEANED)                     │
│                                                                                        │
│  [ dbt Models: dbt_project/models/silver/ ]                                            │
│    ├── dim_customer (Deduplication via ROW_NUMBER, SHA2 surrogate keys, clean types)   │
│    ├── dim_account  (Account status & timestamps)                                      │
│    ├── dim_date     (Pre-calculated calendar dimensions: day, week, month, quarter)    │
│    ├── dim_branch   (Branch entities)                                                  │
│    ├── fact_loan    (Cleaned loan balances, interest rates, safe numeric double casts) │
│    └── fact_transaction (Txn metrics, fraud detection flags, foreign key resolution)   │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                3. GOLD LAYER (BUSINESS ANALYTICS)                      │
│                                                                                        │
│  [ dbt Models: dbt_project/models/gold/ ]                                              │
│    ├── Executive Monthly Performance Scorecards                                        │
│    ├── Month-over-Month (MoM) Growth Rates using LAG() window functions                │
│    ├── Risk & Subprime Loan Default Analysis                                           │
│    └── Customer Lifetime Value & Segment Rankings using RANK() / DENSE_RANK()          │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Pre-Configured Credentials & Environment

All files in `D:\capstone\` are pre-wired with these verified credentials:

| Setting / Variable | Configured Value | Purpose |
| :--- | :--- | :--- |
| **`SNOWFLAKE_ACCOUNT`** | `RULVIWR-NBB14569` | Snowflake Organization & Account identifier |
| **`SNOWFLAKE_USER`** | `NIRAJ21` | Login user |
| **`SNOWFLAKE_PASSWORD`** | `max33@DUDUDUDU` | Login password |
| **`SNOWFLAKE_ROLE`** | `ACCOUNTADMIN` | Execution role |
| **`SNOWFLAKE_WAREHOUSE`** | `BANKING_WH` | Elastic virtual warehouse |
| **`SNOWFLAKE_DATABASE`** | `CAPSTONE_DB` | Target database |
| **Airflow Connection ID** | `snowflake_default` | Pre-configured Airflow connection |

---

## 3. Phase 1: Snowflake Environment Setup

Open **[app.snowflake.com](https://app.snowflake.com)** in your browser, log in as `NIRAJ21`, switch role to `ACCOUNTADMIN`, open a **SQL Worksheet**, and run:

```sql
USE ROLE ACCOUNTADMIN;

-- 1. Create Virtual Warehouse (auto-suspends after 5 min to save credits)
CREATE WAREHOUSE IF NOT EXISTS BANKING_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Elastic warehouse for Capstone data pipeline';

-- 2. Create Target Database
CREATE DATABASE IF NOT EXISTS CAPSTONE_DB;
USE DATABASE CAPSTONE_DB;

-- 3. Create Medallion Schemas
CREATE SCHEMA IF NOT EXISTS BRONZE COMMENT = 'Raw Ingested Data Layer';
CREATE SCHEMA IF NOT EXISTS SILVER COMMENT = 'Cleaned & Conformed Dimensional Layer';
CREATE SCHEMA IF NOT EXISTS GOLD COMMENT = 'Business Aggregations & KPIs';

-- 4. Create CSV File Format
USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT csv_file_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- 5. Create Internal Named Stage
CREATE OR REPLACE STAGE my_stage
    FILE_FORMAT = csv_file_format
    COMMENT = 'Internal landing stage for uploading raw CSV files';

-- 6. Create Audit Tracking Table
CREATE OR REPLACE TABLE BRONZE.FILE_AUDIT (
    AUDIT_ID INT AUTOINCREMENT,
    FILENAME VARCHAR(500) NOT NULL,
    ROWSLOADED INT DEFAULT 0,
    STATUS VARCHAR(500) NOT NULL,
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (AUDIT_ID)
);

-- 7. Create Your Raw Table (Example schema; customize column names to your dataset)
CREATE OR REPLACE TABLE BRONZE.RAW_DATA (
    COL1 VARCHAR(255),
    COL2 VARCHAR(255),
    COL3 VARCHAR(255),
    COL4 VARCHAR(255),
    COL5 VARCHAR(255),
    SOURCEFILE VARCHAR(500),
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

---

## 4. Phase 2: Launching Your Local Airflow Environment

### 4.1. Open Terminal & Start Astronomer
```powershell
cd D:\capstone
astro dev start
```
*What happens automatically:*
1. Docker builds the Astronomer runtime with `snowflake-connector-python`, `dbt-snowflake`, and `astronomer-cosmos`.
2. Astro reads `airflow_settings.yaml` and auto-provisions the `snowflake_default` connection.
3. Airflow webserver starts at [http://localhost:8080](http://localhost:8080) (or dynamic port).

### 4.2. Run the Connection Verification DAG
1. In the Airflow UI, trigger `test_snowflake_connection`.
2. Inspect the log of `test_snowflake_ping` and verify the status is **SUCCESS**.

---

## 5. Phase 3: Defining Your Raw Ingestion DAG (Bronze Layer)

Create your ingestion DAG at `D:\capstone\dags\ingest_and_transform_pipeline.py`:

```python
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

# Paths inside Docker container
INPUT_FOLDER = Path("/usr/local/airflow/data/my_drop_folder")
DBT_PROJECT_PATH = "/usr/local/airflow/dbt_project"
DBT_PROFILES_PATH = "/usr/local/airflow/dbt_project/profiles.yml"
SNOWFLAKE_CONN_ID = "snowflake_default"

def check_for_new_files():
    """Checks if there are any un-processed CSV files in the input directory."""
    files = list(INPUT_FOLDER.glob("*.csv"))
    if not files:
        print("No CSV files found.")
        return False

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    pending_files = []
    for f in files:
        cursor.execute(
            "SELECT COUNT(*) FROM BRONZE.FILE_AUDIT WHERE FILENAME = %s AND STATUS = 'SUCCESS'",
            (f.name,)
        )
        if cursor.fetchone()[0] == 0:
            pending_files.append(f.name)

    cursor.close()
    conn.close()

    if pending_files:
        print(f"New pending files to ingest: {pending_files}")
        return True
    return False

def load_files_to_snowflake():
    """Uploads CSV to Snowflake stage via PUT and streams to table via COPY INTO."""
    files = list(INPUT_FOLDER.glob("*.csv"))
    if not files:
        return

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    for f in files:
        # Check if already successfully ingested
        cursor.execute(
            "SELECT COUNT(*) FROM BRONZE.FILE_AUDIT WHERE FILENAME = %s AND STATUS = 'SUCCESS'",
            (f.name,)
        )
        if cursor.fetchone()[0] > 0:
            print(f"Skipping {f.name} - already ingested.")
            continue

        file_posix_path = f.resolve().as_posix()
        try:
            # 1. High-speed GZIP upload to internal stage
            print(f"Staging {f.name}...")
            cursor.execute(f"PUT 'file://{file_posix_path}' @BRONZE.my_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

            # 2. Bulk load with file metadata injection
            print(f"Executing COPY INTO for {f.name}...")
            copy_sql = f"""
            COPY INTO BRONZE.RAW_DATA (COL1, COL2, COL3, COL4, COL5, SOURCEFILE)
            FROM (
                SELECT t.$1, t.$2, t.$3, t.$4, t.$5, '{f.name}'
                FROM @BRONZE.my_stage/{f.name} t
            )
            FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FILE_FORMAT')
            ON_ERROR = 'CONTINUE';
            """
            cursor.execute(copy_sql)

            # 3. Query loaded row count
            cursor.execute("SELECT COUNT(*) FROM BRONZE.RAW_DATA WHERE SOURCEFILE = %s", (f.name,))
            rows_loaded = cursor.fetchone()[0]

            # 4. Insert success record in audit log
            cursor.execute(
                "INSERT INTO BRONZE.FILE_AUDIT (FILENAME, ROWSLOADED, STATUS) VALUES (%s, %s, %s)",
                (f.name, rows_loaded, "SUCCESS")
            )
            conn.commit()
            print(f"Successfully loaded {rows_loaded} rows from {f.name}.")

        except Exception as e:
            conn.rollback()
            cursor.execute(
                "INSERT INTO BRONZE.FILE_AUDIT (FILENAME, ROWSLOADED, STATUS) VALUES (%s, %s, %s)",
                (f.name, 0, f"FAILED: {str(e)[:450]}")
            )
            conn.commit()
            raise e

    cursor.close()
    conn.close()

with DAG(
    dag_id="ingest_and_transform_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "medallion", "dbt"],
) as dag:

    wait_for_files = PythonSensor(
        task_id="wait_for_files",
        python_callable=check_for_new_files,
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )

    load_raw_data = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_files_to_snowflake,
    )

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="capstone_dbt",
            target_name="dev",
            profiles_yml_filepath=DBT_PROFILES_PATH,
        ),
    )

    wait_for_files >> load_raw_data >> dbt_transform
```

---

## 6. Phase 4: Building Your Dimensional & Fact Models (Silver Layer)

### 6.1. Declare Bronze Source in `dbt_project/models/sources.yml`
```yaml
version: 2

sources:
  - name: bronze
    database: "{{ env_var('SNOWFLAKE_DATABASE', 'CAPSTONE_DB') }}"
    schema: BRONZE
    tables:
      - name: RAW_DATA
        description: "Raw ingested dataset"
```

### 6.2. Create Dimension Model: `dbt_project/models/silver/dim_entity.sql`
```sql
{{ config(materialized='view', schema='silver') }}

WITH raw_source AS (
    SELECT
        COL1 AS entity_id,
        COL2 AS entity_name,
        COL3 AS transaction_date,
        COL4 AS category,
        COL5 AS amount_str
    FROM {{ source('bronze', 'RAW_DATA') }}
    WHERE COL1 IS NOT NULL AND TRIM(COL1) <> ''
),

cleaned AS (
    SELECT
        -- 1. Cryptographic SHA256 Surrogate Key
        SHA2(TRIM(entity_id), 256) AS entity_key,
        TRY_CAST(TRIM(entity_id) AS VARCHAR(50)) AS entity_id,
        TRY_CAST(NULLIF(TRIM(entity_name), '') AS VARCHAR(100)) AS entity_name,
        
        -- 2. Multi-Format Resilient Date Parsing
        COALESCE(
            TRY_TO_DATE(TRIM(transaction_date), 'YYYY-MM-DD'),
            TRY_TO_DATE(TRIM(transaction_date), 'MM/DD/YYYY'),
            TRY_TO_DATE(TRIM(transaction_date), 'DD/MM/YYYY'),
            TRY_TO_DATE(TRIM(transaction_date))
        ) AS transaction_date,
        
        TRY_CAST(NULLIF(TRIM(category), '') AS VARCHAR(50)) AS category,
        TRY_CAST(NULLIF(TRIM(amount_str), '') AS DECIMAL(18,2)) AS amount,
        
        -- 3. Deduplication: Select latest record per entity
        ROW_NUMBER() OVER (
            PARTITION BY entity_id 
            ORDER BY transaction_date DESC NULLS LAST
        ) AS row_num
    FROM raw_source
)

SELECT
    entity_key,
    entity_id,
    entity_name,
    transaction_date,
    category,
    amount
FROM cleaned
WHERE row_num = 1
```

---

## 7. Phase 5: Building Business KPIs & Analytics (Gold Layer)

Place all your aggregation views in `D:\capstone\dbt_project\models\gold/`.

### 7.1. KPI Model 1: Volume & Revenue by Category
`dbt_project/models/gold/kpi_category_summary.sql`:
```sql
{{ config(materialized='view', schema='gold') }}

SELECT
    category,
    COUNT(entity_id) AS total_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS average_amount
FROM {{ ref('dim_entity') }}
WHERE category IS NOT NULL
GROUP BY category
```

### 7.2. KPI Model 2: Month-over-Month (MoM) Growth Using `LAG()`
`dbt_project/models/gold/kpi_mom_growth.sql`:
```sql
{{ config(materialized='view', schema='gold') }}

WITH monthly_totals AS (
    SELECT
        TO_VARCHAR(transaction_date, 'YYYY-MM') AS txn_month,
        ROUND(SUM(amount), 2) AS monthly_revenue
    FROM {{ ref('dim_entity') }}
    WHERE transaction_date IS NOT NULL
    GROUP BY TO_VARCHAR(transaction_date, 'YYYY-MM')
),

with_lag AS (
    SELECT
        txn_month,
        monthly_revenue,
        LAG(monthly_revenue) OVER (ORDER BY txn_month) AS prev_month_revenue
    FROM monthly_totals
)

SELECT
    txn_month,
    monthly_revenue,
    prev_month_revenue,
    ROUND(
        CASE
            WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 THEN NULL
            ELSE ((monthly_revenue - prev_month_revenue) * 100.0) / prev_month_revenue
        END,
        2
    ) AS mom_growth_pct
FROM with_lag
```

### 7.3. KPI Model 3: Entity Rankings Using `RANK()` / `DENSE_RANK()`
`dbt_project/models/gold/kpi_top_entities.sql`:
```sql
{{ config(materialized='view', schema='gold') }}

WITH ranked_entities AS (
    SELECT
        category,
        entity_id,
        entity_name,
        amount,
        DENSE_RANK() OVER (
            PARTITION BY category 
            ORDER BY amount DESC
        ) AS rank_in_category
    FROM {{ ref('dim_entity') }}
)

SELECT *
FROM ranked_entities
WHERE rank_in_category <= 5
```

---

## 8. Phase 6: End-to-End Orchestration with Cosmos & Airflow

### 8.1. Running dbt Directly from Terminal (Fast Validation)
To compile and test your models in Snowflake without waiting for an Airflow DAG run:
```bash
# Get your active Airflow scheduler container ID
docker ps

# Run dbt debug
docker exec -w /usr/local/airflow/dbt_project <CONTAINER_ID> dbt debug --profiles-dir /usr/local/airflow/dbt_project

# Build all Silver & Gold models in Snowflake
docker exec -w /usr/local/airflow/dbt_project <CONTAINER_ID> dbt run --profiles-dir /usr/local/airflow/dbt_project
```

### 8.2. Triggering the Full Pipeline via Airflow Web UI
1. Open the Airflow Web UI at [http://localhost:8080](http://localhost:8080).
2. Drop your raw CSV into your drop folder.
3. Unpause and trigger **`ingest_and_transform_pipeline`**.
4. Cosmos will automatically render the dbt dependency tree as native Airflow tasks and execute them sequentially after raw ingestion finishes.

---

## 9. Phase 7: Critical Snowflake SQL Functions & Syntax Cheat Sheet

| Task / Operation | SQL Server (T-SQL) | Snowflake Cloud SQL |
| :--- | :--- | :--- |
| **Surrogate Key Hashing** | `CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', col), 2)` | `SHA2(TRIM(col), 256)` |
| **Multi-Format Date Parsing** | `TRY_CONVERT(DATE, col, 120)` | `COALESCE(TRY_TO_DATE(col, 'YYYY-MM-DD'), ...)` |
| **Timestamp Parsing** | `TRY_CONVERT(DATETIME2, col, 120)` | `COALESCE(TRY_TO_TIMESTAMP(col, 'YYYY-MM-DD HH24:MI:SS'), ...)` |
| **Date Formatting** | `FORMAT(date_col, 'yyyy-MM')` | `TO_VARCHAR(date_col, 'YYYY-MM')` |
| **Integer Date Key** | `CAST(FORMAT(date_col, 'yyyyMMdd') AS INT)` | `CAST(TO_VARCHAR(date_col, 'YYYYMMDD') AS INT)` |
| **Month Name** | `DATENAME(MONTH, date_col)` | `MONTHNAME(date_col)` |
| **Day of Week Name** | `DATENAME(WEEKDAY, date_col)` | `DAYNAME(date_col)` |
| **ISO Day of Week** | `DATEPART(WEEKDAY, date_col)` | `DAYOFWEEKISO(date_col)` *(1=Mon, 7=Sun)* |
| **String Length** | `LEN(col)` | `LENGTH(col)` |
| **Safe Float to Int Cast** | `TRY_CAST(ROUND(TRY_CAST(col AS FLOAT), 0) AS INT)` | `CAST(ROUND(TRY_TO_DOUBLE(col)) AS INT)` |

---

## 10. Phase 8: Verification & Production Troubleshooting Runbook

### Verification Queries (Run in Snowflake Worksheet)
```sql
USE DATABASE CAPSTONE_DB;

-- 1. Check Bronze Ingestion Status & Row Counts
SELECT * FROM BRONZE.FILE_AUDIT ORDER BY LOAD_TIMESTAMP DESC;
SELECT COUNT(*) FROM BRONZE.RAW_DATA;

-- 2. Check Silver Conformed Models
SELECT * FROM SILVER.DIM_ENTITY LIMIT 10;

-- 3. Check Gold Analytics & KPIs
SELECT * FROM GOLD.KPI_CATEGORY_SUMMARY;
SELECT * FROM GOLD.KPI_MOM_GROWTH;
SELECT * FROM GOLD.KPI_TOP_ENTITIES;
```

---

### Common Issues & Quick Fixes

#### 1. "User account has been temporarily locked" (Error 390102)
* **Cause**: 5 consecutive failed logins before password correction.
* **Fix**: In Snowflake Worksheet as `ACCOUNTADMIN`, execute:
  ```sql
  ALTER USER NIRAJ21 SET MINS_TO_UNLOCK = 0;
  ```

#### 2. "Schemas created with PUBLIC_ prefix (e.g. PUBLIC_SILVER)"
* **Cause**: dbt default schema naming macro prepends default schema.
* **Fix**: Ensure `dbt_project/macros/generate_schema_name.sql` exists with:
  ```sql
  {% macro generate_schema_name(custom_schema_name, node) -%}
      {%- set default_schema = target.schema -%}
      {%- if custom_schema_name is none -%}{{ default_schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
  {%- endmacro %}
  ```

#### 3. "Permission denied: '/usr/local/airflow/dbt_project/target'"
* **Cause**: Linux Docker container file permission ownership mismatch.
* **Fix**: In PowerShell run:
  ```powershell
  docker exec -u 0 <SCHEDULER_CONTAINER_ID> chmod -R 777 /usr/local/airflow/dbt_project
  ```

#### 4. "Function TRY_CAST cannot be used with arguments of types FLOAT"
* **Cause**: Applying `TRY_CAST` to an already-numeric float.
* **Fix**: Use `CAST(ROUND(TRY_TO_DOUBLE(column_name)) AS INT)`.
