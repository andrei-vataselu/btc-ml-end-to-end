
 

from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
 
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job_name = args['JOB_NAME']
except Exception: 
    job_name = "btc_daily_quality_manual"

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session


job = Job(glueContext)
job.init(job_name, {})

print("starting")

today = (datetime.utcnow() - timedelta(days=1)).strftime("%Y/%m/%d")
path = f"s3://<your-bucket-name>/raw/stream/year={today[:4]}/month={today[5:7]}/day={today[8:10]}/"
print(f"reading from: {path}")
 
df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(path)
)

print("\n schema:")
df.printSchema()
df.show(5, truncate=False)
 
cols = [c.lower() for c in df.columns]
print(f"\n detected columns: {cols}")

rename_map = {
    "epoch_ms": "time",
    "iso_ts": "iso_ts",
    "price_usd": "price_usd",
    "source": "source",
    "volume": "volume",
    "hour": "hour"   
}
 
select_expr = [f"{col} as {rename_map[col]}" for col in cols if col in rename_map]
df = df.selectExpr(*select_expr)
 
df.printSchema()
df.show(5, truncate=False)
 
total_rows = df.count()
null_prices = df.filter(F.col("price_usd").isNull()).count()
neg_prices = df.filter(F.col("price_usd") < 0).count()
dup_timestamps = df.count() - df.dropDuplicates(["time"]).count()

print(f"total rows ..........: {total_rows}")
print(f"null price_usd ......: {null_prices}")
print(f"negative price_usd ..: {neg_prices}")
print(f"duplicate timestamps : {dup_timestamps}")


if 1000 < total_rows < 2000 and null_prices == 0 and neg_prices == 0 and dup_timestamps == 0:
    status = "PASSED"
    print("\n data quality PASSED for today.")
else:
    status = "FAILED"
    print("\n data quality FAILED — investigate anomalies.")

result_df = spark.createDataFrame(
    [(today, total_rows, null_prices, neg_prices, dup_timestamps, status)],
    ["day", "rows", "null_prices", "neg_prices", "duplicate_timestamps", "status"]
)

out_path = f"s3://<your-bucket-name>/quality_reports/year={today[:4]}/month={today[5:7]}/day={today[8:10]}/"
result_df.write.mode("overwrite").parquet(out_path)

print("\n quality check completed and report saved successfully.")
 
print("\n glue job completed and committed successfully.")

job.commit()