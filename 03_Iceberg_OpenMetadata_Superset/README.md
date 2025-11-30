
# 📊 Part 3 — Data Governance & Visualization

This part of the project focuses on implementing data governance and visualization using:

- **Apache Iceberg**
- **ClickHouse**
- **OpenMetadata**
- **Apache Superset**

---

## 🚀 Project Overview

For Project 3 we added minio as the object storage for the data that is queried through API's. 
Data pipeline in a simplified form:

1. Elering Price API
2. Minio with Iceberg to ensure rollbacks & consistent reads across snapshots
3. Clickhouse as the main database
4. Apache Superset to visualise data

All data is still automated via Apache Airflow and DBT.

---

## 🛠️ Setup & Installation

1. **Install [Tailscale](https://tailscale.com/)**: Sign in to Tailscale with the Google account or invite link provided by the team.
2. **Obtain secrets**: Get the shared `.env.local` file from the team. Place it alongside `docker-compose.yml` inside `02_Airflow_ClickHouse_dbt/` before starting any containers.
The file can be found from [Google Drive](https://drive.google.com/file/d/1_C8yHceYJq4tOPXwc69b1QlV-fdz3qt6/view?usp=sharing) and is directly available to course lectors. The peer graders and other interested parties must request access and **provide well explained reason** to obtain the access.
3. **Clone or update the project**:
  - Fresh setup: `git clone https://github.com/LauriLopp/DE_project_2025.git`
  - Existing clone: `git pull` to fetch the latest changes.
4. **Change directory**: `cd DE_project_2025/02_Airflow_ClickHouse_dbt`
5. **Start the stack**: `docker compose up --build -d`
6. **Access Airflow**: open `http://localhost:8080`, log in with username `airflow` and password `airflow`.
7. **Enable Main DAG**: turn on the `continuous_ingestion_pipeline` DAG and confirm the tasks progress to running state.
8. **Enable Historical DAG**: turn on the `backfill_historical_data` DAG and confirm the tasks progress to running state.
9  **Verify Metadata**: Open http://localhost:8585/ and log in with credentials: admin@open-metadata.org/admin   
10. **Verify that data is in Minio (Object Storage)**: Open `http://localhost:9001/login` , log in with credentials minioadmin/minioadmin
11. **Access Apache Superset UI**: open `http://localhost:8088`, login with credentials: admin/admin
12. **Verify data**: Navigate to dashboard for data visualisations. See screenshots in Example Queries & Dashboards

---

## 📁 Project Structure

*Placeholder: Directory and file layout for this part of the project.*

---

## 🗄️ Data Governance with Apache Iceberg & OpenMetadata

*Placeholder: How data governance is managed, including metadata management, lineage, and cataloging.*

---

## 🏦 Data Storage & Analytics with ClickHouse

*Placeholder: Overview of data storage, querying, and analytics using ClickHouse.*

---

## 📈 Visualization with Apache Superset

For data visualisation, we connected Clickhouse to Apache Superset in the usual manner.
For visualising our API data, we created a single dashboard. 


---

## 🔗 Integration & Workflow

*Placeholder: How the tools are integrated and how data flows through the system.*

---

## 🖼️ Screenshots & Visuals

Elering Price data in Minio
![Elering Price Minio](elering_price_data_parquet_minio.png)

Bronze Elering Price in Clickhouse through Minio/Iceberg
![Elering Price Clickhouse](elering_price_clickhouse.png)

Apache Superset Dashboard
![Apache Superset Dashboard](dashboard_with_data.png)

---

## 📝 Example Queries & Dashboards

Dashboard & Chart descriptions

Electricity price line-chart

![Electricity Price](Electricity_price.jpg)

In October and November we experienced "minus-price-day", where the price was negative. See filtered data:
![Electricity Price 0](Electricity%20Price%200%20in%20Oct-Nov.jpg)


Average Heat-Pump Power vs Outdoor Temperature (5°C bins).
Answers Q1 and Q2 — “How much energy does the AC need at different outdoor temperatures?”

![Power vs Outdoor](average-heat-pump-power-vs-outdoor-temperature-5-c-bins.jpg)

Energy Cost vs Electricity Price Bucket.
Answers Q3 — “How much do price fluctuations impact cost?”

![Cost vs Bucket](how-much-do-price-fluctuations-impact-cost.jpg)

Energy Use vs Temp Difference (Indoor – Outdoor).
Answers Q1 and Q5 — demonstrates heating physics (Temp Delta → Power usage).

![Indoor vs Outdoor](energy-use-vs-temp-difference-indoor-outdoor.jpg)

---

## 📚 References & Further Reading

*Placeholder: Links to documentation, tutorials, and related resources.*

---
