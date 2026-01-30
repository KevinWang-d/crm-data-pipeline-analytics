# Databricks notebook source
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

# COMMAND ----------

print("Hello World!")

# COMMAND ----------

row_counts_df = spark.sql("""
SELECT 'accounts'      AS table_name, COUNT(*) AS row_count FROM bronze_accounts
UNION ALL
SELECT 'customers',    COUNT(*) FROM bronze_customers
UNION ALL
SELECT 'transactions', COUNT(*) FROM bronze_transactions
""")

display(row_counts_df)

# COMMAND ----------

null_accounts_df = spark.sql("""
SELECT
  SUM(CASE WHEN branchNumber IS NULL THEN 1 ELSE 0 END) AS null_branchNumber,
  SUM(CASE WHEN type         IS NULL THEN 1 ELSE 0 END) AS null_type,
  SUM(CASE WHEN openDate     IS NULL THEN 1 ELSE 0 END) AS null_openDate,
  SUM(CASE WHEN id           IS NULL THEN 1 ELSE 0 END) AS null_id,
  SUM(CASE WHEN iban         IS NULL THEN 1 ELSE 0 END) AS null_iban,
  SUM(CASE WHEN balance      IS NULL THEN 1 ELSE 0 END) AS null_balance,
  SUM(CASE WHEN currency     IS NULL THEN 1 ELSE 0 END) AS null_currency,
  SUM(CASE WHEN cust_id      IS NULL THEN 1 ELSE 0 END) AS null_cust_id
FROM bronze_accounts
""")

display(null_accounts_df)

# COMMAND ----------

null_customers_df = spark.sql("""
SELECT
  SUM(CASE WHEN id                              IS NULL THEN 1 ELSE 0 END) AS null_id,
  SUM(CASE WHEN type                            IS NULL THEN 1 ELSE 0 END) AS null_type,
  SUM(CASE WHEN gender                          IS NULL THEN 1 ELSE 0 END) AS null_gender,
  SUM(CASE WHEN birthDate                       IS NULL THEN 1 ELSE 0 END) AS null_birthDate,
  SUM(CASE WHEN workActivity                    IS NULL THEN 1 ELSE 0 END) AS null_workActivity,
  SUM(CASE WHEN occupationIndustry              IS NULL THEN 1 ELSE 0 END) AS null_occupationIndustry,
  SUM(CASE WHEN totalIncome                     IS NULL THEN 1 ELSE 0 END) AS null_totalIncome,
  SUM(CASE WHEN relationshipStatus              IS NULL THEN 1 ELSE 0 END) AS null_relationshipStatus,
  SUM(CASE WHEN habitationStatus                IS NULL THEN 1 ELSE 0 END) AS null_habitationStatus,
  SUM(CASE WHEN addresses_principalResidence_province IS NULL THEN 1 ELSE 0 END) AS null_province,
  SUM(CASE WHEN schoolAttendance                IS NULL THEN 1 ELSE 0 END) AS null_schoolAttendance,
  SUM(CASE WHEN schools                         IS NULL THEN 1 ELSE 0 END) AS null_schools
FROM bronze_customers
""")

display(null_customers_df)

# COMMAND ----------

null_transactions_df = spark.sql("""
SELECT
  SUM(CASE WHEN description         IS NULL THEN 1 ELSE 0 END) AS null_description,
  SUM(CASE WHEN currencyAmount      IS NULL THEN 1 ELSE 0 END) AS null_currencyAmount,
  SUM(CASE WHEN accountId           IS NULL THEN 1 ELSE 0 END) AS null_accountId,
  SUM(CASE WHEN originationDateTime IS NULL THEN 1 ELSE 0 END) AS null_originationDateTime,
  SUM(CASE WHEN categoryTags        IS NULL THEN 1 ELSE 0 END) AS null_categoryTags,
  SUM(CASE WHEN locationRegion      IS NULL THEN 1 ELSE 0 END) AS null_locationRegion,
  SUM(CASE WHEN locationCity        IS NULL THEN 1 ELSE 0 END) AS null_locationCity,
  SUM(CASE WHEN merchantId          IS NULL THEN 1 ELSE 0 END) AS null_merchantId
FROM bronze_transactions
""")

display(null_transactions_df)

# COMMAND ----------

dup_accounts_df = spark.sql("""
SELECT id, COUNT(*) AS cnt
FROM bronze_accounts
GROUP BY id
HAVING COUNT(*) > 1
""")

display(dup_accounts_df)

# COMMAND ----------

dup_customers_df = spark.sql("""
SELECT id, COUNT(*) AS cnt
FROM bronze_customers
GROUP BY id
HAVING COUNT(*) > 1
""")

display(dup_customers_df)

# COMMAND ----------

balance_range_df = spark.sql("""
SELECT
  MIN(CAST(balance AS DOUBLE)) AS min_balance,
  MAX(CAST(balance AS DOUBLE)) AS max_balance
FROM bronze_accounts
""")

display(balance_range_df)

# COMMAND ----------

age_income_range_df = spark.sql("""
SELECT
  -- Age (approx) in whole years
  MIN(FLOOR(datediff(current_date(), to_date(birthDate, 'M/d/yyyy')) / 365)) AS min_age_years,
  MAX(FLOOR(datediff(current_date(), to_date(birthDate, 'M/d/yyyy')) / 365)) AS max_age_years,

  -- Income
  ROUND(MIN(CAST(totalIncome AS DOUBLE)), 2) AS min_totalIncome,
  ROUND(MAX(CAST(totalIncome AS DOUBLE)), 2) AS max_totalIncome
FROM bronze_customers
""")

display(age_income_range_df)


# COMMAND ----------

txn_range_df = spark.sql("""
SELECT
  DATE(MIN(to_timestamp(originationDateTime))) AS min_txn_date,
  DATE(MAX(to_timestamp(originationDateTime))) AS max_txn_date,
  ROUND(MIN(CAST(currencyAmount AS DOUBLE)), 2) AS min_amount,
  ROUND(MAX(CAST(currencyAmount AS DOUBLE)), 2) AS max_amount
FROM bronze_transactions
""")

display(txn_range_df)

# COMMAND ----------

orphan_accounts_df = spark.sql("""
SELECT a.*
FROM bronze_accounts a
LEFT JOIN bronze_customers c
  ON a.cust_id = c.id
WHERE c.id IS NULL
""")

display(orphan_accounts_df);

# COMMAND ----------

orphan_transactions_df = spark.sql("""
SELECT t.*
FROM bronze_transactions t
LEFT JOIN bronze_accounts a
  ON t.accountId = a.id
WHERE a.id IS NULL
""")

display(orphan_transactions_df)