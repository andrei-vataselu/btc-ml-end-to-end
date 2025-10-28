# Stream Guide

This folder holds ingestion code and daily conversion to Parquet.

## Components

- `stream.py`: writes minute-level BTC/USD CSVs to S3 (`raw/stream/`)
- `parquet_convert.py`: daily Lambda that converts CSVs into partitioned Parquet and repairs Athena partitions

## Lambda Configuration

### stream.py

- **Runtime**: Python 3.13
- **Architecture**: x86_64
- **Schedule**: Per minute execution

### parquet_convert.py

- **Runtime**: Python 3.13
- **Architecture**: x86_64
- **Schedule**: Daily cron job

## Flow

1. `stream.py` runs every minute and writes CSV to S3
2. `parquet_convert.py` runs daily and aggregates by day → writes to `processed_partitioned/year=YYYY/`
3. Glue crawler updates partitions

## Notes

- Handles duplicates and missing columns
- Reprocesses missing days when needed
