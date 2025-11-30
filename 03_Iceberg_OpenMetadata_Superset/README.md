
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

*Placeholder: Steps to set up the environment, install dependencies, and configure services.*

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


Bronze Elering Price in Clickhouse through Minio/Iceberg

Apache Superset Dashboard
![Apache Superset Dashboard](dashboard_with_data.png)

---

## 📝 Example Queries & Dashboards

*Placeholder: Example SQL queries and dashboard descriptions for analytics and reporting.*

---

## 📚 References & Further Reading

*Placeholder: Links to documentation, tutorials, and related resources.*

---
