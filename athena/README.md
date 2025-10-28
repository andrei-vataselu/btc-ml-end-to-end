# Athena Guide

This folder contains SQL for database and table setup used by Athena.

## What it does

- Creates the database for BTC analytics
- Defines raw and processed tables
- Repairs partitions after writes

## Files

- `init.sql`: database and table statements

## Quick start

1. Open the Athena query editor
2. Run the statements in `init.sql`
3. Verify with `SELECT * FROM <yourdbname>.processed_partitioned LIMIT 10;`
