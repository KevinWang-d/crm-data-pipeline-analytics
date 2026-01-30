# Databricks notebook source
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")


# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE silver_accounts AS
SELECT
  CAST(branchNumber AS INT) AS branchNumber,
  type,
  to_date(openDate, 'M/d/yyyy') AS openDate,
  id,
  CAST(balance AS DOUBLE) AS balance,
  currency,
  cust_id
FROM bronze_accounts
""")


# COMMAND ----------

display(spark.sql("SELECT * FROM silver_accounts LIMIT 20"))


# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE silver_customers AS
SELECT
  id,
  type,
  gender,
  to_date(birthDate, 'M/d/yyyy') AS birthDate,
  -- Derived age (NULL if unrealistic)
  CASE 
      WHEN FLOOR(datediff(current_date(), to_date(birthDate, 'M/d/yyyy')) / 365)
           BETWEEN 18 AND 100
      THEN FLOOR(datediff(current_date(), to_date(birthDate, 'M/d/yyyy')) / 365)
      ELSE NULL
  END AS age,
  workActivity,
  occupationIndustry,
  CAST(totalIncome AS DOUBLE) AS totalIncome,
  relationshipStatus,
  habitationStatus,
  addresses_principalResidence_province AS province,
  schoolAttendance,
  schools
FROM bronze_customers
""")


# COMMAND ----------

display(spark.sql("SELECT * FROM silver_accounts LIMIT 20"))


# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE silver_transactions AS
SELECT
  accountId,
  description,
  CAST(currencyAmount AS DOUBLE) AS amount,
  to_timestamp(originationDateTime) AS txn_time,
  categoryTags,
  locationRegion,
  locationCity,
  merchantId
FROM bronze_transactions
""")


# COMMAND ----------

display(spark.sql("SELECT * FROM silver_transactions LIMIT 20"))


# COMMAND ----------

silver_counts_df = spark.sql("""
SELECT 'accounts_bronze'      AS table_name, COUNT(*) AS row_count FROM bronze_accounts
UNION ALL
SELECT 'accounts_silver',     COUNT(*) FROM silver_accounts
UNION ALL
SELECT 'customers_bronze',    COUNT(*) FROM bronze_customers
UNION ALL
SELECT 'customers_silver',    COUNT(*) FROM silver_customers
UNION ALL
SELECT 'transactions_bronze', COUNT(*) FROM bronze_transactions
UNION ALL
SELECT 'transactions_silver', COUNT(*) FROM silver_transactions
""")

display(silver_counts_df)


# COMMAND ----------

silver_accounts_nulls = spark.sql("""
SELECT
  SUM(CASE WHEN id        IS NULL THEN 1 ELSE 0 END) AS null_accountId,
  SUM(CASE WHEN cust_id   IS NULL THEN 1 ELSE 0 END) AS null_customerId,
  SUM(CASE WHEN balance   IS NULL THEN 1 ELSE 0 END) AS null_balance,
  SUM(CASE WHEN openDate  IS NULL THEN 1 ELSE 0 END) AS null_openDate
FROM silver_accounts
""")

display(silver_accounts_nulls)


# COMMAND ----------

silver_customers_nulls = spark.sql("""
SELECT
  SUM(CASE WHEN id   IS NULL THEN 1 ELSE 0 END) AS null_customerId,
  SUM(CASE WHEN age  IS NULL THEN 1 ELSE 0 END) AS null_age,
  SUM(CASE WHEN totalIncome IS NULL THEN 1 ELSE 0 END) AS null_totalIncome
FROM silver_customers
""")

display(silver_customers_nulls)


# COMMAND ----------

silver_tx_nulls = spark.sql("""
SELECT
  SUM(CASE WHEN accountId IS NULL THEN 1 ELSE 0 END) AS null_accountId,
  SUM(CASE WHEN amount    IS NULL THEN 1 ELSE 0 END) AS null_amount,
  SUM(CASE WHEN txn_time  IS NULL THEN 1 ELSE 0 END) AS null_txn_time
FROM silver_transactions
""")

display(silver_tx_nulls)


# COMMAND ----------

silver_balance_range = spark.sql("""
SELECT
  MIN(balance) AS min_balance,
  MAX(balance) AS max_balance
FROM silver_accounts
""")

display(silver_balance_range)


# COMMAND ----------

silver_age_income_range = spark.sql("""
SELECT
  MIN(age) AS min_age,
  MAX(age) AS max_age,
  MIN(totalIncome) AS min_income,
  MAX(totalIncome) AS max_income
FROM silver_customers
""")

display(silver_age_income_range)


# COMMAND ----------

silver_txn_range = spark.sql("""
SELECT
  DATE(MIN(txn_time)) AS min_txn_date,
  DATE(MAX(txn_time)) AS max_txn_date,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
FROM silver_transactions
""")

display(silver_txn_range)


# COMMAND ----------

silver_orphan_accounts = spark.sql("""
SELECT a.*
FROM silver_accounts a
LEFT JOIN silver_customers c
  ON a.cust_id = c.id
WHERE c.id IS NULL
""")

display(silver_orphan_accounts)


# COMMAND ----------

silver_orphan_tx = spark.sql("""
SELECT t.*
FROM silver_transactions t
LEFT JOIN silver_accounts a
  ON t.accountId = a.id
WHERE a.id IS NULL
""")

display(silver_orphan_tx)
