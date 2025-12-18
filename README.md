Unified Sales Analytics Lakehouse
📊 Unified Sales Analytics – End-to-End Data Engineering Lakehouse

Real-time ingestion → Delta Lake → Spark ETL → Airflow orchestration → Postgres DW → Grafana dashboards


Overview
Unified Sales Analytics is a complete, production-grade Data Engineering pipeline built locally using:
Apache Kafka – Real-time event ingestion
Apache Spark (Batch & Streaming) – Processing & transformations
Delta Lake – Bronze / Silver / Gold architecture
Apache Airflow – Orchestration
PostgreSQL – Data Warehouse
Grafana – Dashboards & KPIs
The system simulates real e-commerce sales events and transforms them into business-ready metrics such as:
Daily Gross Revenue
Total Orders
Average Order Value
This is a fully functional lakehouse project designed to demonstrate real industry workflows.

How to Run the Project

1️⃣ Create project environment
cd unified-sales-analytics
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

2️⃣ Start Kafka
If using brewed Kafka:
brew services start kafka
brew services start zookeeper

3️⃣ Run Event Producer
python producer/produce_events.py
Events will start flowing into your Kafka topic.

4️⃣ Start Spark Streaming Job (Bronze ingestion)
python spark-jobs/streaming/stream_to_bronze.py
This writes raw events into:
delta-lake/bronze

5️⃣ Initialize Airflow
cd unified-sales-analytics
source airflow-venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate
airflow webserver -p 8080
airflow scheduler

Open Airflow UI:
👉 http://localhost:8080
User: admin
Pass: 12345
Enable the DAG:
sales_batch_pipeline
Run it manually or wait for daily schedule.

6️⃣ Verify Gold metrics
python spark-jobs/batch/gold_to_postgres.py
Check data:
psql -h localhost -U $USER -d unified_sales_analytics
SELECT * FROM sales_daily_metrics;

7️⃣ Dashboard in Grafana
Open Grafana:
👉 http://localhost:3000
Login: admin / admin
Add PostgreSQL Data Source:
Host: localhost
Port: 5432
Database: unified_sales_analytics
User: <your mac username>
Create dashboard panels:
Revenue Trend (gross_revenue)
Order Count Trend (order_count)
Average Order Value
KPIs

📈 Example KPIs
Date	Revenue	Orders	AOV
2025-12-15	61030.83	239	255

🧪 Key Features
✔️ End-to-end orchestration
Airflow runs the entire chain daily.
✔️ Fully modular Spark jobs
Clear separation between Bronze, Silver, Gold.
✔️ Enterprise-level Lakehouse
Delta ACID tables with schema tracking.
✔️ Real-time + batch hybrid
✔️ Grafana dashboards
Perfect for business stakeholders.

🏁 Conclusion
This project showcases the full lifecycle of modern data engineering, including:
Real-time ingestion
Lakehouse modeling
ELT pipelines
Orchestration
Data warehousing
BI dashboards
