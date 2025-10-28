# ML Guide

This folder contains the machine learning pieces for volatility forecasting. It builds on the processed Parquet data and integrates with SageMaker for jobs and a real-time endpoint.

## What’s here

- `training_job.py`: trains a Random Forest on daily features from `processed_partitioned/` and writes artifacts to S3.
- `retrain.py`: Lambda that starts a SageMaker Processing Job to retrain on new data.
- `auto_update.py`: Lambda that updates the SageMaker endpoint to the latest model.

## Data inputs

- Reads from `s3://<your-bucket>/processed_partitioned/year=YYYY/...`
- Requires columns: `day`, `avg_price_usd`, `min_price_usd`, `max_price_usd`, `year`

## Artifacts written

- `s3://<your-bucket>/models/volatility_model/`
  - `volatility_model.joblib` (model)
  - `metrics.json` (MAE, RMSE, R²)
  - `training_manifest.jsonl.gz` (data read log)

## How to run training locally

1. Set environment variables (examples):
   - `bucket=<your-bucket>`
   - `prefix=processed_partitioned`
   - `start_year=2013`
   - `end_year=2025`
   - `exclude_years=2024`
   - `artifact_prefix=models/volatility_model`
2. Run:

```bash
python training_job.py
```

## Retraining flow (AWS)

- Event source: new data or a schedule → triggers `retrain.py` (Lambda)
- `retrain.py` launches a SageMaker Processing Job that executes your training script in a managed container
- Artifacts are written to S3 under `models/volatility_model/`

Env expected by `retrain.py`:

- `SAGEMAKER_ROLE_ARN`: execution role for the job
- `BUCKET`: target S3 bucket
- `PREFIX`: data prefix (default `processed_partitioned`)

## Endpoint update flow (AWS)

- Trigger: model upload or job completion → runs `auto_update.py` (Lambda)
- It creates a new SageMaker Model and Endpoint Config, then switches the endpoint

Env expected by `auto_update.py`:

- `AWS_REGION`: AWS region (e.g., `us-east-1`)
- `ENDPOINT_NAME`: your SageMaker endpoint name
- `MODEL_NAME`: base model name prefix
- `ENDPOINT_CONFIG_PREFIX`: base endpoint config prefix
- `BUCKET`: S3 bucket with artifacts
- `MODEL_ARTIFACT_PREFIX`: path to `volatility_model.joblib`
- `SAGEMAKER_ROLE_ARN`: execution role for the model
- `INFERENCE_IMAGE`: container image for inference
- `CLEANUP_OLD` (optional): `true` to delete older models/configs

## Example inference

```python
import boto3, json

runtime = boto3.client("sagemaker-runtime")
payload = {"avg_price_usd": [29500], "min_price_usd": [28900], "max_price_usd": [30000], "year": [2025]}

resp = runtime.invoke_endpoint(
    EndpointName="btc-volatility-endpoint",
    ContentType="application/json",
    Body=json.dumps(payload)
)
print(resp["Body"].read().decode())
```
