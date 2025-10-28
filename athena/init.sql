-- have in mind in athena execute each query sepparatedly

CREATE DATABASE IF NOT EXISTS < yourdbname > COMMENT 'BTC/USD historical analytics database' LOCATION 's3://<your-bucket-name>/';

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
  "escapeChar" = "\",
  "quoteChar" = "\"
)
LOCATION 's3://your-bucket-name/raw/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL
TABLE IF NOT EXISTS < yourdbname >.processed_partitioned (
    day date,
    avg_price_usd double,
    min_price_usd double,
    max_price_usd double
) PARTITIONED BY (year int) STORED AS PARQUET LOCATION 's3://your-bucket-name/processed_partitioned/' TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

INSERT INTO
    < yourdbname >.processed_partitioned
SELECT
    date(from_unixtime(time / 1000)) AS day,
    avg(close) AS avg_price_usd,
    min(low) AS min_price_usd,
    max(high) AS max_price_usd,
    year(from_unixtime(time / 1000)) AS year
FROM < yourdbname >.parquet_raw
GROUP BY
    1,
    5
ORDER BY 1;

MSCK REPAIR TABLE < yourdbname >.processed_partitioned;

SELECT * FROM < yourdbname >.processed_partitioned LIMIT 10;