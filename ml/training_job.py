import os
import io
import sys
import json
import time
import math
import gzip
import boto3
import traceback
import concurrent.futures as cf
from datetime import datetime, timezone


import pandas as pd
from io import BytesIO 
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error

aws_region = boto3.Session().region_name or os.getenv("aws_region", "<your-aws-region>")
s3 = boto3.client("s3", config=BotoConfig(retries={"max_attempts": 5, "mode": "adaptive"}, read_timeout=60), region_name=aws_region)

bucket = os.getenv("bucket", "<your-bucket-name>")
prefix = os.getenv("prefix", "processed_partitioned")
start_year = int(os.getenv("start_year", "2013"))
end_year = int(os.getenv("end_year", "2025"))
exclude_years = {int(y) for y in os.getenv("exclude_years", "2024").split(",") if y.strip()}

max_workers = int(os.getenv("max_workers", "16"))
s3_max_attempts = int(os.getenv("s3_max_attempts", "3"))
min_rows_to_train = int(os.getenv("min_rows_to_train", "200"))

time_aware_split = os.getenv("time_aware_split", "true").lower() == "true"
test_size = float(os.getenv("test_size", "0.2"))
random_state = int(os.getenv("random_state", "42"))

n_estimators = int(os.getenv("n_estimators", "500"))
max_depth = int(os.getenv("max_depth", "12"))

artifact_prefix = os.getenv("artifact_prefix", "models/volatility_model")
model_name = "volatility_model.joblib"
metrics_name = "metrics.json"
features_name = "feature_importances.csv"
manifest_name = "training_manifest.jsonl.gz"

required_cols = ["day", "avg_price_usd", "min_price_usd", "max_price_usd", "year"]
numeric_cols = ["avg_price_usd", "min_price_usd", "max_price_usd", "year"]
target_col = "daily_range"
features = ["avg_price_usd", "min_price_usd", "max_price_usd", "year"]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def list_s3(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def read_object(bucket, key):
    buf = BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    for read_func in (pd.read_parquet, lambda b: pd.read_csv(b), lambda b: pd.read_csv(b, encoding="latin-1")):
        try:
            return read_func(buf)
        except Exception:
            buf.seek(0)
    raise RuntimeError(f"unreadable file {key}")

def safe_read(args):
    bucket, key = args
    for attempt in range(1, s3_max_attempts + 1):
        try:
            df = read_object(bucket, key)
            return key, df, None
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            time.sleep(min(2 ** attempt, 10))
    return key, None, err

def load_data():
    print(f"scanning s3://{bucket}/{prefix}/ for {start_year}..{end_year} excluding {sorted(exclude_years)}")
    candidates = []
    for key in list_s3(bucket, prefix):
        if "year=" not in key or key.endswith("/"):
            continue
        try:
            year = int(key.split("year=")[1].split("/")[0])
        except:
            continue
        if start_year <= year <= end_year and year not in exclude_years:
            candidates.append((year, key))
    if not candidates:
        raise RuntimeError("no data files found")
    print(f"found {len(candidates)} files across {len(set(y for y, _ in candidates))} years")

    manifest, dfs = [], []
    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        for key, df, err in ex.map(safe_read, [(bucket, k) for _, k in candidates]):
            year = int(key.split("year=")[1].split("/")[0])
            manifest.append({
                "time": now_iso(),
                "key": key,
                "year": year,
                "rows": 0 if df is None else int(df.shape[0]),
                "status": "ok" if err is None else "error",
                "error": err,
            })
            if df is not None:
                df["year"] = year
                dfs.append(df)
    if not dfs:
        raise RuntimeError("all file reads failed")
    df = pd.concat(dfs, ignore_index=True)
    print(f"loaded {df.shape[0]} rows x {df.shape[1]} cols")
    return df, manifest

def clean(df):
    df = df[[c for c in required_cols if c in df.columns]].copy()
    if "day" in df.columns:
        df["day"] = pd.to_datetime(df["day"], errors="coerce")
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df.dropna(subset=["avg_price_usd", "min_price_usd", "max_price_usd", "year"], inplace=True)
    before = len(df)
    df.drop_duplicates(inplace=True)
    df[target_col] = df["max_price_usd"] - df["min_price_usd"]
    df = df[df[target_col] > 0].dropna(subset=[target_col])
    print(f"clean shape {df.shape}")
    return df

def split(df):
    x, y = df[features], df[target_col]
    if time_aware_split and "day" in df.columns and df["day"].notna().any():
        df_sorted = df.sort_values("day")
        n = len(df_sorted)
        n_test = max(1, int(n * test_size))
        x_train, x_test = df_sorted.iloc[:-n_test][features], df_sorted.iloc[-n_test:][features]
        y_train, y_test = df_sorted.iloc[:-n_test][target_col], df_sorted.iloc[-n_test:][target_col]
        print(f"time split train={len(x_train)} test={len(x_test)}")
    else:
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size, random_state=random_state)
        print(f"random split train={len(x_train)} test={len(x_test)}")
    return x_train, x_test, y_train, y_test

def train_eval(x_train, x_test, y_train, y_test):
    model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, random_state=random_state, n_jobs=-1)
    model.fit(x_train, y_train)
    preds = model.predict(x_test)
    metrics = {
        "mae": float(mean_absolute_error(y_test, preds)),
        "rmse": float(math.sqrt(mean_squared_error(y_test, preds))),
        "r2": float(r2_score(y_test, preds))
    }
    print(f"mae={metrics['mae']:.3f} rmse={metrics['rmse']:.3f} r2={metrics['r2']:.3f}")
    return model, metrics

def save_artifacts(model, metrics, manifest):
    import joblib
    model_file = model_name
    joblib.dump(model, model_file)
    model_key = f"{artifact_prefix}/{model_name}"
    metrics_key = f"{artifact_prefix}/{metrics_name}"
    manifest_key = f"{artifact_prefix}/{manifest_name}"

    metrics_payload = {"timestamp": now_iso(), "metrics": metrics, "bucket": bucket, "prefix": prefix}
    manifest_buf = io.BytesIO()
    with gzip.GzipFile(fileobj=manifest_buf, mode="wb") as gz:
        for m in manifest:
            gz.write((json.dumps(m) + "\n").encode("utf-8"))
    manifest_bytes = manifest_buf.getvalue()

    s3.upload_file(model_file, bucket, model_key)
    s3.put_object(Bucket=bucket, Key=metrics_key, Body=json.dumps(metrics_payload, indent=2).encode("utf-8"))
    s3.put_object(Bucket=bucket, Key=manifest_key, Body=manifest_bytes)
    print("upload complete")

def main():
    try:
        df_raw, manifest = load_data()
        df = clean(df_raw)
        if len(df) < min_rows_to_train:
            raise RuntimeError(f"not enough rows ({len(df)} < {min_rows_to_train})")
        x_train, x_test, y_train, y_test = split(df)
        model, metrics = train_eval(x_train, x_test, y_train, y_test)
        save_artifacts(model, metrics, manifest)
        print("training complete and uploaded")
    except Exception as e:
        print("training failed:", e)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
