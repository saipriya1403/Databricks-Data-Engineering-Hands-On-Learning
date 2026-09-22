# Day 14 - Data Quality and Quarantine

## Objective

Learn how to handle invalid customer data using data quality checks and quarantine.

## Pipeline

CSV
↓
Auto Loader
↓
Bronze
↓
Data Quality Check
↓
Valid → Silver
Invalid → Quarantine

## Input File

customers_04.csv

Path:

/Volumes/dataengineering/default/day11_customer_files/day14_customer_files/

## Data Quality Rules

- CustomerId should not be NULL
- CustomerName should not be NULL
- City should not be NULL
- Age should be greater than 0
- UpdatedAt should be a valid timestamp

## Bronze Table

bronze.day14_customers

Bronze stores the incoming customer records.

## Silver Table

silver.day14_customers

Only valid customer records are stored here.

## Quarantine Table

silver.day14_customer_quarantine

Invalid records are stored separately with the reason for failure.

## Test Results

Valid records:

- CustomerId 105 - Meena
- CustomerId 109 - Karthik

Invalid records:

- CustomerId 106 - CustomerName is NULL
- CustomerId 107 - Age must be greater than 0
- CustomerId 108 - City is NULL

## Key Learning

Day 14 introduced data quality validation and quarantine.

Instead of silently dropping bad records, invalid records are stored separately so they can be investigated and corrected.

## Day 14 Flow

CSV → Auto Loader → Bronze → Data Quality → Silver / Quarantine
