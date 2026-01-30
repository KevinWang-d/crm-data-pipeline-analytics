# crm-data-pipeline-analytics
Customer Analytics Pipeline (Bronze → Silver → Gold)

 Overview
This project implements a full data pipeline for customer and transaction analytics using the Medallion Architecture (Bronze, Silver, Gold).

It processes raw CRM and transaction data and produces business-ready analytics and customer segmentation models.

 Business Objective
- Ensure data quality and reliability
- Standardize customer and account records
- Build customer KPIs and RFM metrics
- Support marketing and retention analysis

 Tech Stack
- Python (PySpark)
- Databricks
- Spark SQL
- Machine Learning (KMeans)
- Matplotlib

 Pipeline Architecture

 Bronze Layer — Data Quality & Validation
- Row count checks
- Null and duplicate detection
- Range validation
- Orphan record detection

 Silver Layer — Data Cleaning & Transformation
- Type casting and normalization
- Date standardization
- Derived features (age)
- Schema alignment

Gold Layer — Analytics & Aggregation
- RFM modeling
- Branch performance KPIs
- Monthly trends
- Customer metrics

BI & ML Layer — Segmentation & Insights
- RFM scoring
- Customer segmentation
- KMeans clustering
- Performance visualization
- What-if analysis

Project Structure
bronze/ - Data quality checks
silver/ - Data cleaning & transformations
gold/ - Analytics models
bi_ml/ - Segmentation & ML

 Key Outcomes
- Built validated analytics-ready datasets
- Identified high-value and at-risk customers
- Created customer segments for targeted marketing
- Produced branch and trend performance metrics

 How to Run
This project is designed to run in a Databricks / Spark environment.

Each script represents one pipeline layer and should be executed in sequence:
1. Bronze
2. Silver
3. Gold
4. BI & ML
