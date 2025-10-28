# Dataset Guide

Source: OHLCV BTC 1 minute dataset

This folder includes sample BTC/USD data for testing.

## Files

- `btcusd.csv`: historical ticks with columns `time,open,close,high,low,volume`

## Usage

- Upload to `s3://<your-bucket>/raw/` for batch ingestion
- The streaming flow writes to `raw/stream/` automatically
