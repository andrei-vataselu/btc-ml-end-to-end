import awswrangler as wr
import boto3, os, pandas as pd
from datetime import datetime

s3 = boto3.client("s3")
athena = boto3.client("athena")

BUCKET = os.environ.get("BUCKET", "<your-bucket-name>")
RAW_PREFIX = "raw/stream"
PROC_PREFIX = "processed_partitioned"
DATABASE = "<your-catalog-name>"
TABLE = "processed_partitioned"
ATHENA_OUTPUT = f"s3://{BUCKET}/athena/output/" 
def list_raw_days(bucket, prefix, year):
    paginator = s3.get_paginator("list_objects_v2")
    prefix_path = f"{prefix}/year={year}/month="
    days_found = set()

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_path, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            if "month=" in cp["Prefix"]:
                month_prefix = cp["Prefix"]
                month = int(month_prefix.split("month=")[-1].rstrip("/"))
                for subpage in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, Prefix=month_prefix, Delimiter="/"
                ):
                    for subcp in subpage.get("CommonPrefixes", []):
                        if "day=" in subcp["Prefix"]:
                            day = int(subcp["Prefix"].split("day=")[-1].rstrip("/"))
                            days_found.add(datetime(year, month, day).date())
    return sorted(days_found)

 
def lambda_handler(event, context):
    current_year = datetime.utcnow().year
    today = datetime.utcnow().date()

    parquet_path = f"s3://{BUCKET}/{PROC_PREFIX}/"
 
    try:
        df_existing = wr.s3.read_parquet(path=parquet_path, dataset=True)
        df_existing["day"] = pd.to_datetime(df_existing["day"]).dt.date
        df_existing = df_existing[df_existing["year"] == current_year]
        existing_days = set(df_existing["day"].unique())
        print(f"existing days in parquet for {current_year}: {sorted(existing_days)}")
    except Exception:
        df_existing = pd.DataFrame()
        existing_days = set()

    available_days = set(list_raw_days(BUCKET, RAW_PREFIX, current_year))
    available_days = {d for d in available_days if d < today}  
    print(f"available raw days  for {current_year}: {sorted(available_days)}")
 
    missing_days = sorted(list(available_days - existing_days))
    if not missing_days:
        return {"status": "up_to_date", "year": current_year}

    print(f"missing days to process: {missing_days}")
 
    all_new = []
    for date_to_process in missing_days:
        y, m, d = date_to_process.year, f"{date_to_process.month:02}", f"{date_to_process.day:02}"
        raw_path = f"s3://{BUCKET}/{RAW_PREFIX}/year={y}/month={m}/day={d}/"
        print(f"raw data from: {raw_path}")

        try:
            df_raw = wr.s3.read_csv(raw_path)
        except Exception as e:
            print(f"Could not read {raw_path}: {e}")
            continue

        if df_raw.empty:
            continue

        df_raw = df_raw.drop_duplicates(subset=["iso_ts", "price_usd"])
        df_raw["ts"] = pd.to_datetime(df_raw["iso_ts"])
        daily = (
            df_raw.assign(day=df_raw["ts"].dt.floor("D"))
            .groupby("day")
            .agg(
                avg_price_usd=("price_usd", "mean"),
                min_price_usd=("price_usd", "min"),
                max_price_usd=("price_usd", "max"),
            )
            .reset_index()
        )
        daily["year"] = y
        all_new.append(daily)

    if not all_new:
        return {"status": "no_new_data", "year": current_year}

    df_new_all = pd.concat(all_new, ignore_index=True)

    if not df_existing.empty:
        df_existing = df_existing[~df_existing["day"].isin(df_new_all["day"])]
        merged = pd.concat([df_existing, df_new_all], ignore_index=True)
    else:
        merged = df_new_all

    merged = merged.drop_duplicates(subset=["day"]).sort_values("day")

    wr.s3.to_parquet(
        df=merged,
        path=parquet_path,
        dataset=True,
        mode="overwrite_partitions",   
        compression="snappy",
        partition_cols=["year"],
    )
 
    athena.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {DATABASE}.{TABLE};",
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )

    print(f"updated  for  {current_year}.")
    return {
        "status": "ok",
        "year": current_year,
        "processed_days": [str(d) for d in missing_days],
        "rows_new": len(df_new_all),
        "rows_total": len(merged),
        "parquet_path": parquet_path,
    }
