# Day 16 - SCD Type 1 and SCD Type 2

## Objective

Learn how to handle customer changes using:

* SCD Type 1
* SCD Type 2
* Delta Lake MERGE

---

## Input

Initial file:

`customers_initial.csv`

Update file:

`customers_updates.csv`

Volume path:

`/Volumes/dataengineering/default/day16_customer_files/`

---

## Initial Customer Data

Customers:

* 401 - Arun - Chennai - Age 30
* 402 - Priya - Bangalore - Age 27
* 403 - Karthik - Salem - Age 35

---

## Update Data

Updates:

* 401 - Arun - Coimbatore - Age 31
* 402 - Priya - Bangalore - Age 28
* 404 - Meena - Madurai - Age 29

Customer 401 changed city and age.

Customer 402 changed age.

Customer 404 is a new customer.

---

## Bronze Table

`bronze.day16_customers`

The initial customer data was stored in a Bronze Delta table.

---

# SCD Type 1

## Silver Table

`silver.day16_customers_scd1`

SCD Type 1 overwrites the existing customer information.

### Result

* Customer 401 → updated to Coimbatore, Age 31
* Customer 402 → updated to Age 28
* Customer 403 → unchanged
* Customer 404 → inserted

The old values of 401 and 402 are not preserved.

---

# SCD Type 2
