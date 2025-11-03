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

## Project Summary

This project project has currently two main parts, that are created to serve as both - individual project and part of overall, greater project.

### Part 1: Data Warehouse Design and Exploration

**(Folder: `01_Business_Brief_and_Star_Shema/`)**

This initial phase focused on business analysis, data exploration, and architectural design. Key activities included:
- **Exploring Raw Data**: Analyzing CSV files containing IoT sensor readings, weather data, and electricity prices to understand their structure and content.
- **Designing a Star Schema**: Modeling a robust data warehouse schema to support analytical queries. This involved defining fact and dimension tables to track energy usage against various environmental and market factors.
- **Writing Demo Queries**: Crafting initial SQL queries to validate the schema design and demonstrate its analytical capabilities.

### Part 2: Automated ELT Pipeline with Airflow, dbt, and ClickHouse

**(Folder: `02_Airflow_ClickHouse_dbt/`)**

The second phase focused on implementing and automating the designed data warehouse. This involved building a modern, containerized ELT (Extract, Load, Transform) pipeline. Key activities included:
- **Containerization**: Using Docker and Docker Compose to define and manage all services, including Airflow, ClickHouse, and dbt.
- **Orchestration**: Building an Airflow DAG to automate the entire workflow, from ingesting raw data from APIs to triggering transformations.
- **Transformation**: Developing a dbt project to transform raw data into a clean, analytics-ready format, populating the staging (Silver) and mart (Gold) layers of the data warehouse.
- **Documentation**: Creating detailed setup and run instructions to ensure the project is reproducible. For a full guide on running the pipeline, see the [Part 2 README](./02_Airflow_ClickHouse_dbt/README.md).

---

## Project structure:
```
DE_project_2025/
├── .git/                                 # Git version control directory
├── .gitignore                          # Specifies files and directories for Git to ignore
├── 01_Business_Brief_and_Star_Shema/     # Part 1: Initial data exploration and schema design
│   ├── demo_queries.sql
│   ├── IOT_andmed_7.12.24-17.03.25.csv
│   ├── iot_data_exploration.ipynb
│   ├── Meteo Physicum archive 071224-170325 - archive.csv
│   ├── NP tunnihinnad    071224-170325.csv
│   ├── P1_Group_21.pdf
│   ├── Star_schema.mmd
│   └── Star_schema.png
├── 02_Airflow_ClickHouse_dbt/              # Part 2: The complete, automated data pipeline
│   ├── .env.local                          # (User-provided) Contains secret tokens and credentials
│   ├── clickhouse-init/                    # Scripts to initialize ClickHouse on first run
│   ├── cloudbeaver-init/                   # Pre-configured connection settings for CloudBeaver UI
│   ├── config/                             # ClickHouse user and profile configurations
│   ├── dags/
│   │   └── home_assistant_continuous_raw.py  # The main Airflow DAG orchestrating the pipeline
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── marts/                      # Gold layer: Dimensional and fact models
│   │   │   │   ├── dim_device.sql
│   │   │   │   ├── dim_location.sql
│   │   │   │   ├── dim_time.sql
│   │   │   │   ├── fact_heating_energy_usage.sql
│   │   │   │   └── schema.yml
│   │   │   └── staging/                    # Silver layer: Cleaned and standardized views
│   │   │       ├── stg_device.sql
│   │   │       ├── stg_iot_data.sql
│   │   │       ├── stg_location.sql
│   │   │       ├── stg_price_data.sql
│   │   │       └── stg_weather_data.sql
│   │   ├── seeds/
│   │   │   └── estonian_holidays.csv
│   │   ├── dbt_project.yml
│   │   ├── packages.yml
│   │   └── profiles.yml
│   ├── device_location_data/               # Static CSVs mounted into ClickHouse for seeding
│   │   ├── device_data.csv
│   │   └── location_data.csv
│   ├── docker-compose.yml                  # Defines and configures all services
│   ├── Dockerfile                          # For the standalone dbt service
│   ├── Dockerfile.airflow                  # For the Airflow services
│   └── README.md                           # Documentation for Part 2
└── README.md                               # Top-level project README
```