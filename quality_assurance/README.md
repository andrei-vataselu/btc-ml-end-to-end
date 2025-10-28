# Quality Assurance Guide

This folder contains checks to validate data quality after conversion.

## What it checks

- Schema consistency
- Nulls and negatives
- Duplicates

## Files

- `script.py`: Glue job that runs the checks and writes reports to `s3://<your-bucket>/quality_reports/`
- `Quality checks.ipynb`: notebook with explorations and visuals
