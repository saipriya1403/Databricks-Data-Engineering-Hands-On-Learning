# Day 15 - Auto Loader to Bronze

## Objective

Learn how to use Auto Loader to read new CSV files and load them into a Bronze Delta table.

## Input

File:

customers_05.csv

Path:

/Volumes/dataengineering/default/day11_customer_files/day15_customer_files/

## Pipeline

CSV
↓
Auto Loader
↓
Bronze Delta Table

## Bronze Table

bronze.day15_customers

## Data

5 customer records were loaded.

Customer IDs:

301, 302, 303, 304, 305

## Key Learning

- Used Auto Loader to read CSV files
- Used an explicit schema
- Used Structured Streaming
- Loaded data into a Bronze Delta table
- Used a checkpoint location for incremental processing

## Verification

Verified the Bronze table using SELECT and COUNT queries.

Total records: 5
