import json
import csv
import io
import urllib.request
import boto3
from datetime import datetime, timezone
import os

# ---------- config for streaming ----------
S3 = boto3.client("s3")
BUCKET = os.environ.get("BUCKET", "<your-bucket-name>")      
PREFIX = os.environ.get("PREFIX", "raw/stream")             
# ----------------------------

def get_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read().decode())
    return float(data["bitcoin"]["usd"])

def lambda_handler(event, context):
    try:
        price = get_price()
    except Exception as e:
        print(f"price fetch failed: {e}")
        return {"status": "error", "message": str(e)}

    ts = datetime.now(timezone.utc)
    epoch_ms = int(ts.timestamp() * 1000)
 
    y, m, d, H, M = ts.strftime("%Y %m %d %H %M").split()
    key = f"{PREFIX}/year={y}/month={m}/day={d}/hour={H}/btc_{y}{m}{d}_{H}{M}.csv"
 
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["epoch_ms", "iso_ts", "price_usd", "source"])
    writer.writerow([epoch_ms, ts.isoformat(), f"{price:.2f}", "coingecko"])

    try:
        S3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )
        print(f"saved to s3://{BUCKET}/{key}")
        return {"status": "ok", "price": price, "s3_key": key}
    except Exception as e:
        print(f"upload failed: {e}")
        return {"status": "error", "message": str(e)}
