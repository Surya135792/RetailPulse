🛒 RetailPulse - Snowflake Data Pipeline Project

📌 Overview

RetailPulse is a scalable data pipeline built using Snowflake that processes retail data from AWS S3 into analytical insights using a Bronze-Silver-Gold architecture.

---

🧱 Architecture

- Bronze Layer: Raw data ingestion using Snowpipe
- Silver Layer: Data cleaning, transformation, deduplication
- Gold Layer: Business-ready dimensional model (Fact & Dimension tables)

---

⚙️ Technologies Used

- Snowflake (Warehouse, Streams, Tasks)
- AWS S3 (Data Storage)
- Snowpipe (Auto Ingestion)
- SQL (ETL transformations)

---

🔄 Data Pipeline Flow

1. Data uploaded to AWS S3
2. Snowpipe loads data into Bronze tables
3. Streams capture incremental changes
4. Tasks process data into Silver layer
5. Gold layer builds fact & dimension tables
6. Analytics tables generated for reporting

---

🔥 Key Features

- Incremental Data Processing using Streams
- Automated Pipelines using Tasks
- SCD Type-2 Implementation (Customer Dimension)
- Data Quality Handling (Rejected Layer)
- Batch Rollback using source_file tracking

---

📊 Analytics

- Total Sales KPI
- Sales by Store
- Top Products
- Customer Segmentation
- Daily Sales Trend

---

🚀 How to Run

1. Execute SQL scripts in order:
   
   - 01_setup.sql
   - 02_bronze_layer.sql
   - 03_silver_layer.sql
   - 04_gold_layer.sql
   - 05_tasks_and_streams.sql
   - 06_analytics.sql

2. Upload data into S3 bucket

3. Trigger Snowpipe (or use AUTO_INGEST)

4. Resume tasks

---

Explanation (CRITICAL) :

👉 “Our project RetailPulse is a real-time retail data pipeline built using Snowflake.”

👉 “We ingest data from AWS S3 using Snowpipe into the Bronze layer.”

👉 “Using Streams and Tasks, we process incremental data into the Silver layer for cleaning and transformation.”

👉 “In the Gold layer, we implement dimensional modeling with Fact and Dimension tables including SCD Type-2 for customer tracking.”

👉 “Finally, we generate business insights like total sales, top products, and customer segmentation.”


**Flow Diagram (Must)
Use:**
draw.io (diagrams.net)

S3 → Snowpipe → Bronze → Streams → Tasks → Silver → Streams → Tasks → Gold → Analytics


🏆 Outcome

A fully automated, scalable, and production-ready ETL pipeline for retail analytics.

---

👨‍💻 Author

Surya
