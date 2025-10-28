# Bitcoin Price Analytics Pipeline with ML

## Contents

- [Overview](#overview)
- [Architecture Summary](#architecture)
- [Dataset Description](#dataset)
  - [Raw CSV Schema](#dataset-raw)
  - [Stream Schema](#dataset-stream)
- [Step 1: Create S3 Structure](#s3-structure)
- [Step 2: AWS Glue Catalog and Crawler](#glue-crawler)
- [Step 3: Create Athena Database](#athena-db)
- [Step 4: Create Raw Table](#raw-table)
- [Step 5: Convert & Aggregate Data into Parquet](#parquet)
- [Step 6: Verify in Athena](#verify)
- [Step 7: Glue Crawler for Processed Data](#crawler-processed)
- [Step 8: Streaming and Lambda Components](#streaming)
- [Step 9: SageMaker Integration `./ml/*`](#ml)
  - [ML Guide](ml/README.md)
- [Folder Guides](#folder-guides)
- [Bibliography](#bibliography)

<a id="overview"></a>

## Overview

This repository documents an AWS-based analytics pipeline for processing Bitcoin (BTC/USD) pricing data.
The workflow converts raw a CSV stored in Amazon S3 into partitioned Parquet datasets optimized for Athena queries and QuickSight visualization, having also the capability of near real time ingestion. **( mini-batch )**

---

<a id="architecture"></a>

## Architecture Summary ( Batch + Mini-Batch Ingestion )

**Data Flow:**

```
S3 (raw btcusd.csv / stream/)
   ↓
AWS Glue Crawler → Glue Data Catalog
   ↓
AWS Athena (SQL Transformations)
   ↓
S3 (processed_partitioned/year=YYYY/) -> SageMaker (Training + Inference)
   ↓
QuickSight (Dashboards)
```

---

<a id="dataset"></a>

## Dataset Description

<a id="dataset-raw"></a>

### Raw CSV Schema (`dataset/btcusd.csv`)

Each CSV file represents historical BTC/USD price ticks.

| Column | Type   | Description                    |
| ------ | ------ | ------------------------------ |
| time   | BIGINT | Unix timestamp in milliseconds |
| open   | DOUBLE | Opening price                  |
| close  | DOUBLE | Closing price                  |
| high   | DOUBLE | Highest price                  |
| low    | DOUBLE | Lowest price                   |
| volume | DOUBLE | Traded volume                  |

<a id="dataset-stream"></a>

### Stream Schema (`stream/stream.py` output)

Each CSV file represents a minute-level BTC/USD price tick written by the streaming Lambda.

| Column    | Type   | Description                    |
| --------- | ------ | ------------------------------ |
| epoch_ms  | BIGINT | Unix timestamp in milliseconds |
| iso_ts    | STRING | ISO 8601 UTC timestamp         |
| price_usd | DOUBLE | Current BTC price in USD       |
| source    | STRING | Data source (e.g. coingecko)   |

---

<a id="s3-structure"></a>

## Step 1: Create S3 Structure

```
s3://<your-bucket-name>/raw/
s3://<your-bucket-name>/processed_partitioned/
s3://<your-bucket-name>/quality_reports/
s3://<your-bucket-name>/models/volatility_model/
```

Upload your BTC CSV files into the `raw/` folder for batch ingestion.
For streaming ingestion, Lambda automatically writes CSVs into `raw/stream/` every minute.

---

<a id="glue-crawler"></a>

## Step 2: AWS Glue Catalog and Crawler

1. Open **AWS Glue → Crawlers → Create crawler**
2. Name: `<your-bucket-name>`
3. Source type: **S3**
   - Include path: `s3://<your-bucket-name>/raw/`
4. Choose IAM Role: `AWSGlueServiceRoleDefault`
5. Output Database: `your-database-name`
6. Run crawler (the type should be on demand).

This automatically creates the table `parquet_raw` in the Glue Data Catalog.

---

<a id="athena-db"></a>

## Step 3: Create Athena Database

```sql
CREATE DATABASE IF NOT EXISTS <yourdbname>
COMMENT 'BTC/USD historical analytics database'
LOCATION 's3://<your-bucket-name>/';
```

---

<a id="raw-table"></a>

## Step 4: Create Raw Table (Glue-generated equivalent)

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS <yourdbname>.parquet_raw (
  time bigint,
  open double,
  close double,
  high double,
  low double,
  volume double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "escapeChar" = "\\",
  "quoteChar" = "\""
)
LOCATION 's3://your-bucket-name/raw/'
TBLPROPERTIES ('skip.header.line.count'='1');
```

---

<a id="parquet"></a>

## Step 5: Convert & Aggregate Data into Parquet

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS <yourdbname>.processed_partitioned (
  day date,
  avg_price_usd double,
  min_price_usd double,
  max_price_usd double
)
PARTITIONED BY (year int)
STORED AS PARQUET
LOCATION 's3://your-bucket-name/processed_partitioned/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

Then aggregate from raw to daily summary:

```sql
INSERT INTO <yourdbname>.processed_partitioned
SELECT
  date(from_unixtime(time/1000)) AS day,
  avg(close) AS avg_price_usd,
  min(low) AS min_price_usd,
  max(high) AS max_price_usd,
  year(from_unixtime(time/1000)) AS year
FROM <yourdbname>.parquet_raw
GROUP BY 1,5
ORDER BY 1;
```

Repair partitions:

```sql
MSCK REPAIR TABLE <yourdbname>.processed_partitioned;
```

---

<a id="verify"></a>

## Step 6: Verify in Athena

```sql
SELECT * FROM <yourdbname>.processed_partitioned LIMIT 10;
```

Expected columns:

```
day | avg_price_usd | min_price_usd | max_price_usd | year
```

---

<a id="crawler-processed"></a>

## Step 7: Glue Crawler for Processed Data

Create another crawler `btcprice-processed-crawler`:

- Source: `s3://your-bucket-name/processed_partitioned/`
- Database: `<yourdbname>`
- Schedule: daily (optional)

Run crawler → updates partition metadata automatically.

---

<a id="streaming"></a>

## Step 8: Streaming and Lambda Components

### 1. Stream Ingestion (Minute-level)

- File: `stream/stream.py`
- Trigger: **Runs every minute**
- Purpose: Writes BTC/USD minute-level CSV data to S3 (`raw/stream/`)

### 2. Daily Parquet Conversion

- File: `stream/parquet_convert.py`
- Trigger: **Daily scheduled Lambda**
- Purpose: Converts daily mini-batch CSVs into partitioned Parquet files.
- Features:
  - Handles duplicates and missing columns
  - Detects missing raw days and reprocesses
  - Writes output to `processed_partitioned/`
  - Repairs Athena partitions automatically

### 3. Quality Assurance Script

- File: `quality_assurance/script.py`
- Type: **Glue ETL Job**
- Trigger: **Runs after daily parquet conversion**
- Purpose: Validates data quality for the latest raw stream
  - Checks schema, nulls, negatives, duplicates
  - Writes daily validation reports to `s3://<your-bucket-name>/quality_reports/`

---

<a id="ml"></a>

## Step 9: SageMaker Integration `./ml/*`

SageMaker provides a continuous ML pipeline for volatility forecasting.

### 9.1 Training Script

- Loads processed data from S3 (`processed_partitioned/`)
- Cleans and merges multi-year data
- Trains a Random Forest model predicting daily volatility
- Saves artifacts to S3 (`models/volatility_model/`):
  - `volatility_model.joblib`
  - `metrics.json`
  - `training_manifest.jsonl.gz`

Reference: [AWS SageMaker Processing Jobs](https://docs.aws.amazon.com/sagemaker/latest/dg/processing-job.html)

### 9.2 Automatic Retraining

- Triggered by new data via **S3 → EventBridge → Lambda**
- Launches a new Processing Job (`training_job.py`)
- Produces versioned model artifacts under `models/volatility_model/`

Reference: [Automating SageMaker with EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-sagemaker.html)

### 9.3 Automated Endpoint Update

- Creates a new model from the latest S3 artifact
- Builds a new endpoint configuration
- Updates the endpoint to serve the latest model
- Waits until `InService`
- Optionally cleans up older models/configs

Trigger options:

- SageMaker job completion via EventBridge
- S3 upload to `models/volatility_model/`

Reference: [SageMaker Real-Time Endpoints](https://docs.aws.amazon.com/sagemaker/latest/dg/realtime-endpoints.html)

### 9.4 Endpoint Inference Example

```python
import boto3, json

runtime = boto3.client("sagemaker-runtime")
payload = {"avg_price_usd": [29500], "min_price_usd": [28900], "max_price_usd": [30000], "year": [2025]}

response = runtime.invoke_endpoint(
    EndpointName="btc-volatility-endpoint",
    ContentType="application/json",
    Body=json.dumps(payload)
)
print(response["Body"].read().decode())
```

### 9.5 Monitoring and Metrics

- Logs MAE, RMSE, R² and model metadata
- Metrics stored at `s3://<your-bucket-name>/models/volatility_model/metrics.json`
- Viewable in QuickSight or CloudWatch

---

## Folder Guides

- `ml/` — ML pipeline and endpoint: [ml/README.md](ml/README.md)
- `athena/` — SQL setup and helpers: [athena/README.md](athena/README.md)
- `stream/` — ingestion and parquet conversion: [stream/README.md](stream/README.md)
- `quality_assurance/` — data checks: [quality_assurance/README.md](quality_assurance/README.md)
- `dataset/` — sample data: [dataset/README.md](dataset/README.md)
- `quicksight/` — dashboard assets: [quicksight/README.md](quicksight/README.md)

## Bibliography

1. [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
2. [AWS Athena Documentation](https://docs.aws.amazon.com/athena/latest/ug/what-is.html)
3. [AWS Lambda Developer Guide](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html)
4. [Amazon SageMaker Developer Guide](https://docs.aws.amazon.com/sagemaker/latest/dg/whatis.html)
5. [Amazon QuickSight Documentation](https://docs.aws.amazon.com/quicksight/latest/user/welcome.html)
