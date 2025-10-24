# Predictive Control of Air-Source Heat Pump Using Environmental and Market Data


This project focuses on designing and building a **data warehouse and analytical pipeline** for monitoring and optimizing the performance of an **air-source heat pump (ASHP)** in a residential setting.

The goal of this project is to plan and conduct working data pipeline from data ingestions to visualisation, using various tools.

---

## Project Objectives

- **Model a Star Schema** to support analytical queries on hourly energy usage.
- **Integrate multiple data sources** — IoT device data, weather data, and market electricity prices.
- **Apply data quality and SCD (Slowly Changing Dimension) logic** to ensure consistent historical tracking.
- **Build a scalable ELT pipeline** using modern data engineering tools (Airflow, dbt, ClickHouse).
- (**Enable visualization and insights** through tools like Apache Superset.)

---

## Project structure:
```
DE_project_2025/
├─ 01_Business_Brief_and_Star_Shema/
│  ├─ demo_queries.sql
│  ├─ IOT_andmed_7.12.24-17.03.25.csv
│  ├─ iot_data_exploration.ipynb
│  ├─ Meteo Physicum archive 071224-170325 - archive.csv
│  ├─ NP tunnihinnad    071224-170325.csv
│  ├─ P1_Group_21.pdf
│  ├─ Star_schema.mmd
│  └─ Star_schema.png
├─ 02_Airflow_ClickHouse_dbt/
│  └─ README.md
├─ README.md
├─ .gitignore
└─ .git/
```