# Databricks notebook source
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")


# COMMAND ----------

rfm = spark.table("gold_customer_rfm")
segments = spark.table("gold_customer_segments")
branch = spark.table("gold_branch_summary")
monthly = spark.table("gold_monthly_trend")


# COMMAND ----------

from pyspark.sql import functions as F

rfm = spark.table("workspace.default.gold_customer_rfm")
display(rfm.limit(10))


# COMMAND ----------

from pyspark.sql.window import Window

w_r = Window.orderBy(F.col("recency_days"))      # lower recency = better
w_f = Window.orderBy(F.col("frequency"))         # higher = better
w_m = Window.orderBy(F.col("monetary"))           # higher = better

rfm_scored = (
    rfm
    .withColumn("r_score_raw", F.ntile(5).over(w_r))
    .withColumn("r_score", 6 - F.col("r_score_raw"))  # invert recency
    .withColumn("f_score", F.ntile(5).over(w_f))
    .withColumn("m_score", F.ntile(5).over(w_m))
    .drop("r_score_raw")
)

display(rfm_scored.limit(10))


# COMMAND ----------

segments = (
    rfm_scored
    .withColumn(
        "segment",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "Champions")
         .when((F.col("r_score") >= 4) & (F.col("f_score") >= 3), "Loyal")
         .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), "New / Promising")
         .when((F.col("r_score") <= 2) & (F.col("m_score") >= 4), "At Risk (High Value)")
         .otherwise("Standard")
    )
)

display(segments.groupBy("segment").count())


# COMMAND ----------

segments.write.mode("overwrite").saveAsTable("workspace.default.gold_customer_segments")


# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.default LIKE 'gold_customer_%'"))


# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

tables = spark.catalog.listTables("default")
[t.name for t in tables if t.name.startswith("gold_")]




# COMMAND ----------

spark.catalog.tableExists("workspace.default.gold_customer_segments")
spark.catalog.tableExists("workspace.default.gold_customer_rfm")


# COMMAND ----------

display(
    spark.table("workspace.default.gold_customer_segments").limit(10)
)


# COMMAND ----------

segments = spark.table("workspace.default.gold_customer_segments")

segment_summary = (
    segments
    .groupBy("segment")
    .count()
    .orderBy("count", ascending=False)
)

display(segment_summary)


# COMMAND ----------

seg_counts = (
    segments
    .groupBy("segment")
    .count()
    .orderBy("count", ascending=False)
)

display(seg_counts)


# COMMAND ----------


from pyspark.sql import functions as F

segments = spark.table("workspace.default.gold_customer_segments")
rfm = spark.table("workspace.default.gold_customer_rfm")

seg_value = (
    segments.select("customerId", "segment")   # keep only what we need
    .join(rfm.select("customerId", "monetary"), on="customerId", how="inner")
    .groupBy("segment")
    .agg(
        F.countDistinct("customerId").alias("customers"),
        F.sum("monetary").alias("total_spend"),
        F.avg("monetary").alias("avg_spend_per_customer")
    )
    .orderBy(F.col("total_spend").desc())
)

display(seg_value)


# COMMAND ----------

pretty_seg_value = (
    seg_value
    .withColumn("total_spend", F.format_number(F.col("total_spend"), 2))
    .withColumn("avg_spend_per_customer", F.format_number(F.col("avg_spend_per_customer"), 2))
)

display(pretty_seg_value)


# COMMAND ----------

monthly = spark.table("workspace.default.gold_monthly_trend")

print(monthly.columns)
display(monthly)


# COMMAND ----------

from pyspark.sql import functions as F

monthly_clean = (
    monthly
    .withColumn("month", F.to_date(F.col("month")))
    .orderBy("month")
)

display(monthly_clean)


# COMMAND ----------

monthly_pdf = monthly_clean.toPandas()

import matplotlib.pyplot as plt

plt.figure(figsize=(10,5))
plt.plot(
    monthly_pdf["month"],
    monthly_pdf["total_amount"],
    marker="o"
)

plt.title("Monthly Total Transaction Amount")
plt.xlabel("Month")
plt.ylabel("Total Spend")
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()


# COMMAND ----------

monthly = spark.table("workspace.default.gold_monthly_trend")

from pyspark.sql import functions as F

monthly_clean = (
    monthly
    .withColumn("total_amount", F.round("total_amount", 2))
    .orderBy("month")
)

monthly_pdf = monthly_clean.toPandas()

import matplotlib.pyplot as plt

fig, ax1 = plt.subplots(figsize=(10,5))

# Total spend (value)
ax1.plot(
    monthly_pdf["month"],
    monthly_pdf["total_amount"],
    marker="o",
    color="blue",
    label="Total Spend"
)
ax1.set_ylabel("Total Spend")

# Transaction volume
ax2 = ax1.twinx()
ax2.plot(
    monthly_pdf["month"],
    monthly_pdf["transactions"],
    marker="x",
    color="orange",
    label="Transactions"
)
ax2.set_ylabel("Transaction Count")

plt.title("Monthly Transaction Volume and Total Spend")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

branch = spark.table("workspace.default.gold_branch_summary")

display(branch)


# COMMAND ----------

branch_sorted = branch.orderBy("total_amount", ascending=False)

display(branch_sorted)


# COMMAND ----------

from pyspark.sql import functions as F

branch_clean = (
    branch
    .withColumn("total_amount", F.round("total_amount", 2))
    .withColumn("avg_txn_amount", F.round("avg_txn_amount", 2))
    .orderBy("total_amount", ascending=False)
)

display(branch_clean)


# COMMAND ----------

branch_pdf = branch_clean.toPandas()

import matplotlib.pyplot as plt

plt.figure(figsize=(10,5))
plt.bar(branch_pdf["branchNumber"], branch_pdf["total_amount"])
plt.title("Total Transaction Amount by Branch")
plt.xlabel("Branch")
plt.ylabel("Total Amount")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

branch_pdf = branch_clean.toPandas()

topN = 20
branch_top = branch_pdf.sort_values("total_amount", ascending=False).head(topN)

import matplotlib.pyplot as plt

plt.figure(figsize=(12,5))
plt.bar(branch_top["branchNumber"].astype(str), branch_top["total_amount"])
plt.title(f"Top {topN} Branches by Total Transaction Amount")
plt.xlabel("Branch")
plt.ylabel("Total Amount")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.show()


# COMMAND ----------

rfm = spark.table("workspace.default.gold_customer_rfm")

display(
    rfm.select(
        "recency_days",
        "frequency",
        "monetary",
        "avg_txn_amount"
    )
)


# COMMAND ----------

display(
    rfm.select(
        "recency_days",
        "frequency",
        "monetary",
        "avg_txn_amount"
    ).summary("count", "min", "max", "mean", "stddev")
)


# COMMAND ----------

rfm_pdf = rfm.select(
    "recency_days",
    "frequency",
    "monetary"
).toPandas()

import matplotlib.pyplot as plt

rfm_pdf.hist(
    bins=30,
    figsize=(12,4),
    layout=(1,3)
)

plt.suptitle("RFM Metric Distributions", fontsize=14)
plt.tight_layout()
plt.show()


# COMMAND ----------

segments = spark.table("workspace.default.gold_customer_segments")
rfm = spark.table("workspace.default.gold_customer_rfm")


# COMMAND ----------

from pyspark.sql import functions as F

segments = spark.table("workspace.default.gold_customer_segments")
rfm      = spark.table("workspace.default.gold_customer_rfm")

seg = segments.select("customerId", "segment").alias("seg")
rfm2 = rfm.select("customerId", "monetary").alias("rfm")

# Overall average customer spend
overall_avg = rfm2.agg(F.avg("monetary").alias("overall_avg_spend")).first()["overall_avg_spend"]

# Segment average vs overall average
seg_perf = (
    seg.join(rfm2, on="customerId", how="inner")
       .groupBy("segment")
       .agg(
           F.count("*").alias("customers"),
           F.round(F.avg("monetary"), 2).alias("avg_spend"),
           F.round(F.sum("monetary"), 2).alias("total_spend")
       )
       .withColumn("overall_avg_spend", F.lit(overall_avg))
       .withColumn("delta_vs_overall", F.round(F.col("avg_spend") - F.col("overall_avg_spend"), 2))
       .withColumn(
           "above_below",
           F.when(F.col("delta_vs_overall") >= 0, F.lit("Above avg")).otherwise(F.lit("Below avg"))
       )
       .orderBy(F.col("avg_spend").desc())
)

display(seg_perf)


# COMMAND ----------

from pyspark.sql import functions as F

segments = spark.table("workspace.default.gold_customer_segments").alias("s")
rfm      = spark.table("workspace.default.gold_customer_rfm").alias("r")

# Build seg_value without ambiguity (only keep r.monetary)
seg_value = (
    segments.select(F.col("s.customerId"), F.col("s.segment"))
    .join(rfm.select(F.col("r.customerId"), F.col("r.monetary")), on="customerId", how="inner")
    .groupBy("segment")
    .agg(
        F.count("*").alias("customers"),
        F.round(F.sum("monetary"), 2).alias("total_spend"),
        F.round(F.avg("monetary"), 2).alias("avg_spend_per_customer")
    )
    .orderBy(F.col("total_spend").desc())
)

display(seg_value)


# COMMAND ----------

what_if = (
    seg_value
    .withColumn(
        "what_if_10pct_uplift",
        F.round(F.col("total_spend") * 1.10, 2)
    )
)

display(what_if)


# COMMAND ----------

from pyspark.sql import functions as F

customers = spark.table("workspace.default.silver_customers")
tx = spark.table("workspace.default.silver_transactions")


# COMMAND ----------

# pick the correct time column name in your silver_transactions
print(tx.columns)


# COMMAND ----------

from pyspark.sql import functions as F

tx = spark.table("workspace.default.silver_transactions")
accts = spark.table("workspace.default.silver_accounts")

print("tx:", tx.columns)
print("accts:", accts.columns)


# COMMAND ----------

from pyspark.sql import functions as F

tx = spark.table("workspace.default.silver_transactions")
accts = spark.table("workspace.default.silver_accounts")

tx_joined = (
    tx.alias("t")
    .join(
        accts.select(
            F.col("id").alias("accountId"),
            F.col("cust_id").alias("customerId")
        ).alias("a"),
        on="accountId",
        how="left"
    )
)

display(tx_joined.select("accountId","customerId","txn_time","amount").limit(10))


# COMMAND ----------

tx_feat = (
    tx_joined
    .select(
        "customerId",
        F.to_timestamp("txn_time").alias("txn_ts"),
        F.col("amount").cast("double").alias("amount")
    )
    .filter(F.col("customerId").isNotNull() & F.col("txn_ts").isNotNull())
)

max_ts = tx_feat.agg(F.max("txn_ts").alias("max_ts")).collect()[0]["max_ts"]

rfm = (
    tx_feat.groupBy("customerId")
    .agg(
        F.datediff(F.lit(max_ts), F.max("txn_ts")).alias("recency_days"),
        F.count("*").alias("frequency"),
        F.sum("amount").alias("monetary"),
        F.avg("amount").alias("avg_txn_amount")
    )
)

display(rfm.orderBy("monetary", ascending=False).limit(10))


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

feature_cols = ["recency_days", "frequency", "monetary"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=5, seed=42)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])
model = pipeline.fit(rfm)
pred = model.transform(rfm)

cluster_profile = (
    pred.groupBy("cluster")
    .agg(
        F.count("*").alias("customers"),
        F.round(F.avg("recency_days"), 2).alias("avg_recency_days"),
        F.round(F.avg("frequency"), 2).alias("avg_frequency"),
        F.round(F.avg("monetary"), 2).alias("avg_monetary")
    )
    .orderBy("cluster")
)

display(cluster_profile)

pred.select("customerId","cluster","recency_days","frequency","monetary","avg_txn_amount") \
    .write.mode("overwrite").saveAsTable("workspace.default.gold_customer_clusters")

cluster_profile.write.mode("overwrite").saveAsTable("workspace.default.gold_cluster_profile")


# COMMAND ----------

# Cluster 0 -> Active Regular Customers
# Cluster 1 -> Dormant / At Risk
# Cluster 2 -> Occasional
# Cluster 3 -> Loyal High-Frequency
# Cluster 4 -> High-Value VIP



# COMMAND ----------

import matplotlib.pyplot as plt

pdf = pred.select("frequency", "monetary", "cluster").toPandas()

plt.figure(figsize=(8,6))
plt.scatter(pdf["frequency"], pdf["monetary"], c=pdf["cluster"], alpha=0.6)
plt.xlabel("Frequency")
plt.ylabel("Monetary")
plt.title("Customer Clusters (RFM)")
plt.show()


# COMMAND ----------

pred = pred.withColumn(
    "is_high_value",
    (pred["cluster"] == 4).cast("int")
)


# COMMAND ----------

from pyspark.sql import functions as F

# cluster_profile should already exist from your ML step
# If not, tell me and I’ll help you recreate it.

cluster_summary = (
    cluster_profile
    .select(
        "cluster",
        F.col("customers"),
        F.round("avg_recency_days", 1).alias("avg_recency_days"),
        F.round("avg_frequency", 1).alias("avg_frequency"),
        F.round("avg_monetary", 2).alias("avg_monetary"),
    )
    .orderBy("cluster")
)

display(cluster_summary)


# COMMAND ----------

from pyspark.sql import functions as F

cluster_labels = {
    0: "Active Regular (Core Base)",
    1: "Dormant / At-Risk",
    2: "Occasional / Cooling",
    3: "Super Active (High Frequency)",
    4: "VIP High-Value Spenders"
}

# pred is your dataframe with 'customerId' and 'cluster'
pred_labeled = pred.withColumn(
    "cluster_label",
    F.create_map([F.lit(x) for kv in cluster_labels.items() for x in kv])[F.col("cluster")]
)

display(pred_labeled.select("customerId","cluster","cluster_label").limit(10))


# COMMAND ----------

cluster_counts = (
    pred_labeled
    .groupBy("cluster_label")
    .count()
    .orderBy("count", ascending=False)
)

display(cluster_counts)


# COMMAND ----------

pdf = cluster_counts.toPandas()

import matplotlib.pyplot as plt

plt.figure(figsize=(8,5))
plt.bar(pdf["cluster_label"], pdf["count"])
plt.xticks(rotation=30)
plt.title("Customer Distribution by Cluster")
plt.ylabel("Number of Customers")
plt.tight_layout()
plt.show()


# COMMAND ----------

scatter_pdf = pred_labeled.select(
    "frequency", "monetary", "cluster_label"
).toPandas()

plt.figure(figsize=(8,6))

for label in scatter_pdf["cluster_label"].unique():
    subset = scatter_pdf[scatter_pdf["cluster_label"] == label]
    plt.scatter(
        subset["frequency"],
        subset["monetary"],
        label=label,
        alpha=0.6
    )

plt.xlabel("Frequency")
plt.ylabel("Monetary")
plt.title("Customer Clusters (RFM)")
plt.legend()
plt.tight_layout()
plt.show()
