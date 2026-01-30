# Databricks notebook source
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")


# COMMAND ----------

accounts = spark.table("silver_accounts")
customers = spark.table("silver_customers")
tx = spark.table("silver_transactions")

display(accounts.limit(5))
display(customers.limit(5))
display(tx.limit(5))


# COMMAND ----------

from pyspark.sql.functions import col

tx_enriched = (
    tx.alias("t")
    .join(accounts.alias("a"), col("t.accountId") == col("a.id"), "inner")
    .select(
        col("t.accountId").alias("accountId"),
        col("a.cust_id").alias("customerId"),
        col("a.branchNumber").alias("branchNumber"),
        col("a.type").alias("accountType"),
        col("t.amount").alias("amount"),
        col("t.txn_time").alias("txn_time"),
        col("t.categoryTags").alias("categoryTags"),
        col("t.merchantId").alias("merchantId"),
        col("t.locationRegion").alias("locationRegion"),
        col("t.locationCity").alias("locationCity"),
    )
)

display(tx_enriched.limit(10))


# COMMAND ----------

tx_enriched.createOrReplaceTempView("tx_enriched")


# COMMAND ----------

from pyspark.sql.functions import (
    col, max as fmax, count as fcount, sum as fsum, avg as favg,
    datediff, current_date, to_date
)

rfm_base = (
    tx_enriched
    .groupBy("customerId")
    .agg(
        fmax("txn_time").alias("last_txn_time"),
        fcount("*").alias("frequency"),
        fsum("amount").alias("monetary"),
        favg("amount").alias("avg_txn_amount"),
    )
)

gold_customer_rfm = (
    rfm_base
    .withColumn("recency_days", datediff(current_date(), to_date(col("last_txn_time"))))
    .select("customerId", "recency_days", "frequency", "monetary", "avg_txn_amount")
)

display(gold_customer_rfm.orderBy(col("monetary").desc()).limit(20))

gold_customer_rfm.write.mode("overwrite").saveAsTable("gold_customer_rfm")


# COMMAND ----------

from pyspark.sql.functions import countDistinct

gold_branch_summary = (
    tx_enriched
    .groupBy("branchNumber")
    .agg(
        countDistinct("customerId").alias("customers"),
        countDistinct("accountId").alias("accounts"),
        fcount("*").alias("transactions"),
        fsum("amount").alias("total_amount"),
        favg("amount").alias("avg_txn_amount"),
    )
)

display(gold_branch_summary.orderBy(col("total_amount").desc()).limit(20))

gold_branch_summary.write.mode("overwrite").saveAsTable("gold_branch_summary")


# COMMAND ----------

from pyspark.sql.functions import date_trunc

gold_monthly_trend = (
    tx_enriched
    .groupBy(date_trunc("month", col("txn_time")).alias("month"))
    .agg(
        fcount("*").alias("transactions"),
        fsum("amount").alias("total_amount"),
        favg("amount").alias("avg_amount"),
    )
    .orderBy("month")
)

display(gold_monthly_trend)

gold_monthly_trend.write.mode("overwrite").saveAsTable("gold_monthly_trend")


# COMMAND ----------

display(spark.sql("SHOW TABLES LIKE 'gold_%'"))


# COMMAND ----------

display(spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema"))


# COMMAND ----------

display(spark.sql("SHOW TABLES"))


# COMMAND ----------

gold_customer_rfm.write.mode("overwrite").saveAsTable("workspace.default.gold_customer_rfm")
gold_branch_summary.write.mode("overwrite").saveAsTable("workspace.default.gold_branch_summary")
gold_monthly_trend.write.mode("overwrite").saveAsTable("workspace.default.gold_monthly_trend")


# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.default LIKE 'gold_*'"))


# COMMAND ----------

