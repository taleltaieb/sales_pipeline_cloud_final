# 5-Step Pipeline to Surface 4 Key Retail KPIs  

A full-stack cloud analytics project that tracks sales performance across multiple stores and item categories using a real-world dataset. This project demonstrates cloud data engineering, data modeling, and advanced business intelligence capabilities.

---

## 🔗 Demo

**Video demo:**  
![Demo of dashboard](Animation.gif)

---

## 🚀 Overview

**Goal:** Build an end-to-end cloud analytics pipeline to analyze and visualize retail sales data using a modern data stack.

---


## 🧱 Architecture Overview
**Pipeline Stack:**
This is a high-level view of the cloud analytics pipeline:

![Simple Architecture Diagram](diagram1.jpg)



---

## 📦 Dataset

* Source: Kaggle - "Predict Future Sales"
* Files:

  * `sales_train.csv`
  * `items.csv`
  * `item_categories.csv`
  * `shops.csv`
* Size: \~3 million rows

---

## 🔧 Pipeline Breakdown

### 1. **Raw Data Ingestion** (Airflow)

* Custom Airflow DAG downloads CSV files from a public S3 bucket
* Loads each dataset into Snowflake's `RAW` schema
* Uses PythonOperator with Snowflake connector

### 2. **Staging Models** (dbt)

* Standardize and clean raw data into the `STAGING` schema
* Convert dates and map Russian labels to English
* Validate data quality using dbt tests

### 3. **Mart Models** (dbt)

* Built into `STAGING_MARTS` schema
* Aggregated models:

  * Daily & monthly sales
  * Revenue by month / shop / category
  * Top products & categories
  * Shop-category matrix
  * Revenue variance metrics

### 4. **Dashboard App** (Streamlit)

* Fully interactive frontend connected to Snowflake in real time
* Cached queries for speed and efficiency
* Hosted on always-on AWS EC2 instance

---

## 📊 Dashboard Features

### 🔹 Top KPIs

* Total Revenue
* Total Items Sold
* Active Shops
* MoM Revenue Change %

### 🔹 Time Series

* Monthly Revenue Trend
* Monthly Quantity Sold

### 🔹 Breakdown Charts

* Revenue by Category (Bar)
* Top Shops by Revenue (Bar)
* Revenue by Shop-Category (Treemap)

### 🔹 Advanced Insights

* Price vs Quantity Correlation (Scatter)
* Monthly Item Consistency (Heatmap)
* Shop Radar Profiles (Radar chart)
* New vs Old Items Trend (Line)

### 🔹 Interactivity

* Month filter (single or range)
* Shop / Category filters
* Revenue vs Volume toggle

---

## 🛠 Deployment

* Hosted on **AWS EC2** (Ubuntu)
* Systemd used to auto-restart on reboot/failure
* UFW configured to expose on custom port
* Virtualenv used for Streamlit dependencies

---

## 💡 Business Value

This project simulates what a client-facing retail analytics solution would look like. It helps answer:

* Which shops are top performers or underperforming?
* How stable is each shop's revenue stream?
* Which categories drive the most revenue or volume?
* Are new items gaining traction compared to legacy products?

---

## 📁 Folder Structure

```
project/
├── airflow/                  # DAG to ingest S3 -> Snowflake
├── dbt_sales_pipeline/       # dbt project (models, staging, marts)
├── streamlit_dashboard/       # Streamlit app files
└── README.md
```

---

## 🧠 Technical Flow

The diagram below shows all tools, data flow, environments, and layers in detail:

![Full Technical Diagram](diagram2.jpg)



* Data Engineering: Airflow, Snowflake, S3
* Data Modeling: dbt, SQL testing, normalization
* Dashboarding: Streamlit, Plotly, Altair
* Deployment: EC2, Linux, ports, virtualenv, systemd
* Business Insight: KPI design, chart selection, storytelling

---

## 📘 Lessons Learned

### ✅ What Went Right
- **End-to-end cloud stack**  
  Built a fully cloud-native pipeline: S3 → Airflow → Snowflake → dbt → Streamlit.  
- **Business-first mindset**  
  Focused on solving real retail challenges (e.g. seasonal demand spikes, shop-level KPIs) rather than “just testing algorithms.”  
- **Modular, reusable code**  
  Shared functions, configs and SQL macros across DAGs, dbt models and the Streamlit dashboard.  
- **Self-hosted, always-on dashboard**  
  Deployed on AWS EC2 with `systemd` and `nginx`, avoiding downtime or sleep-mode limits of free services.

### 🧩 Challenges & Solutions

1. **Hardcoded Credentials**  
   - *Issue:* Snowflake password and profiles.yml values were initially in plain text.  
   - *Fix:* Moved all secrets into a `.env` (gitignored) and load via `os.environ`, ensuring no credentials land in Git.

2. **DAG Failures (S3 & Snowflake Permissions)**  
   - *Issue:* 403 Forbidden when downloading from S3; permission errors when creating schemas/tables in Snowflake.  
   - *Fix:*  
     - Updated S3 bucket policy for public data access.  
     - Pre-created Snowflake database/schema/role bindings.  
     - Verified Airflow connections with `dbt debug` and the Airflow UI.

3. **Empty Tables on Load**  
   - *Issue:* CSVs with special characters/encodings and header misalignment led to zero-row loads.  
   - *Fix:*  
     - Explicitly set `FIELD_OPTIONALLY_ENCLOSED_BY='"'`, `SKIP_HEADER=1`, and UTF-8 encoding in the COPY command.  
     - Added dbt staging tests to assert expected row counts.

4. **Dashboard Persistence & Performance**  
   - *Issue:* Streamlit apps hosted locally or on free tiers “sleep” after inactivity.  
   - *Fix:*  
     - Hosted on an EC2 instance with `systemd` for auto-restart on reboot.  
     - Front-ended with `nginx` for clean URLs, SSL and firewall control.

5. **Basic Visuals Lacked Depth**  
   - *Issue:* Initial bar/line charts didn’t tell a complete story.  
   - *Fix:*  
     - Added radar charts for multi-KPI shop profiles.  
     - Built treemaps to show category contributions.  
     - Created heatmaps for monthly sales consistency and interactive filters for dynamic exploration.

6. **Resource Constraints on EC2**  
   - *Issue:* Small instance RAM caused slow boots and dbt compile failures.  
   - *Fix:*  
     - Upgraded to a `t3.medium` instance.  
     - Leveraged `@st.cache_data` and limited query sizes to improve responsiveness.

### 💡 Key Takeaways
- Treat your portfolio projects like production systems: secure, tested, observable and user-focused.  
- Plan architecture, iterate and validate each step before scaling up.  
- Critically evaluate every visualization: is it clear, relevant, and driving business insight?  

---

## 🌐 Live Links

- 📊 [Interactive Streamlit Dashboard](http://16.171.242.247/)
- 📘 [dbt Documentation](https://taleltaieb.github.io/sales_pipeline_cloud_final/#!/overview)
- 📂 [GitHub Repository](https://github.com/taleltaieb/sales_pipeline_cloud_final)

---

## 🙋‍♂️ Author

**Talel Taieb**

* [LinkedIn](https://www.linkedin.com/in/talel-taieb/)
* [Portfolio](https://taleltaieb.github.io/)
