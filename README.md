# team19
# NYC Taxi Demand Pipeline

## Overview

This project builds an end-to-end data pipeline to analyze how weather conditions influence taxi demand in New York City.

The pipeline:

1. ingests raw NYC taxi trip data and weather data
2. processes and aggregates the data using PySpark
3. writes processed outputs to Google Cloud Storage (GCS)
4. loads the processed outputs into Snowflake as a star schema
5. supports downstream analytics and visualization in Tableau

This repository is organized so that, after configuring your own cloud resources and credentials, you can run the full pipeline from a single entry point.

---

# Repository Structure

```text
team19/
│
├── Spark/
│   ├── spark-job.py
│   └── README.md
│
├── Snowflake/
│   ├── snowflake_load.sql
│   └── README.md
│
├── Luigi/
│   ├── pipeline.py
│   └── README.md
│
├── .env.example
└── README.md
```

---

# Raw Data Sources

The project uses two datasets:

## 1. NYC Yellow Taxi Trip Data

Download source:

- NYC TLC Trip Record Data portal:  
  `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`

You should download the Yellow Taxi parquet files used in this project and place them into a directory such as:

```text
data/raw/NYC-taxi/
```

Example file naming pattern:

```text
yellow_tripdata_2023-01.parquet
yellow_tripdata_2023-02.parquet
...
```

## 2. Weather Data

Download source:

- NOAA Climate Data Online:  
  `https://www.ncei.noaa.gov/cdo-web/`

Export the weather data used for this project as a CSV and place it at:

```text
data/raw/Weather.csv
```

Expected weather columns include:

```text
date
PRCP
SNOW
TAVG
```

---

# Accounts and Services Required

This project uses external cloud services. You will need to create your own accounts and resources.

## 1. Google Cloud Platform / Google Cloud Storage

Sign up:

- `https://cloud.google.com/`

Create your own GCS bucket, for example:

```text
gs://your-bucket-name/
```

This project expects raw and processed data to be stored in your bucket.

## 2. Snowflake

Sign up for a Snowflake account or free trial:

- `https://signup.snowflake.com/`

You will need:

- account identifier
- username
- password
- warehouse
- database
- schema

## 3. Tableau Public

If you want to publish or view the final dashboard:

- `https://public.tableau.com/`

---

# Environment Configuration

Before running the pipeline, copy the example environment file:

```bash
cp .env.example .env
```

Then edit `.env` with your own values.

Example:

```bash
GCP_PROJECT=your-gcp-project-id
GCP_REGION=us-central1
GCP_CLUSTER=your-dataproc-cluster-name

GCS_BUCKET=gs://your-bucket-name
RAW_TAXI_PATH=NYC-taxi
RAW_WEATHER_PATH=Weather.csv

SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

---

# All Code Included in This Repository

This repository includes all code written for the project, including one-time setup and warehouse creation.

## Spark Code

- `Spark/spark-job.py`  
  Main PySpark ETL pipeline

## Snowflake Code

- `Snowflake/snowflake_load.sql`  
  One-time setup and data warehouse loading script, including:
  - database creation
  - schema creation
  - storage integration
  - external stage
  - fact/dimension table creation
  - `COPY INTO` loading

## Luigi Script

- `Luigi/pipeline.py`  
  Luigi task orchestration

## Root Entrypoint

- `pipeline.py`  
  Main entrypoint for running the full pipeline from the repository root


---

# What the Pipeline Produces

The Spark pipeline generates the following processed tables:

```text
fact_taxi_demand
dim_time
dim_weather
dim_location
```

These are written to your configured GCS bucket.

The Snowflake layer then loads these outputs into:

```text
NYCTAXI_DB.STAR_SCHEMA
```

---

# How to Run the Full Pipeline

Before running the pipeline:

1. download the raw taxi parquet files and weather CSV from the links above
2. upload both raw datasets into your own GCS bucket
3. create your own Dataproc cluster in the same GCP project
4. copy `.env.example` to `.env` and fill in your own GCP and Snowflake values
5. authenticate `gcloud` to the same project used in `.env`
6. install SnowSQL locally

After those setup steps are complete, run:

```bash
./pipeline.py
```

This script is the main entrypoint for the full pipeline.

At a high level, it:

1. runs the Spark ETL pipeline
2. writes processed outputs to Google Cloud Storage (GCS)
3. runs the Snowflake SQL setup/load script
4. prepares the final fact and dimension tables for analytics

Important:

- the raw input data must already exist in your own GCS bucket before you run `./pipeline.py`
- `GCP_CLUSTER` must refer to an existing Dataproc cluster
- the pipeline submits `Spark/spark-job.py` from your local repository to Dataproc

---

# Notes on Credentials

This repository does **not** include any tokens, passwords, or private credentials.

To run the project, you must create your own:

- GCS bucket
- Snowflake account and credentials
- Snowflake warehouse/database/schema

---

# Tableau Dashboard

Tableau dashboard link:

```text
https://public.tableau.com/app/profile/zihan.ye/viz/405FinalProjectNYCTaxiWeatherDashboard/Dashboard1
```

---

# Assumptions and Adaptation Notes

Because users will not have access to our original GCS bucket or Snowflake account, the project is designed to be portable:

- replace the bucket name in configuration
- create your own Snowflake resources
- run the provided scripts in order

The code is written so that, after the environment is configured, the full pipeline can be executed from one entrypoint script.

---

# Local `.env` Setup and Run

Create your local config from the example:

```bash
cp .env.example .env
```

Then fill in at least these values in `.env`:

```bash
GCP_PROJECT=your-gcp-project-id
GCP_REGION=us-central1
GCP_CLUSTER=your-dataproc-cluster-name

GCS_BUCKET=gs://your-bucket-name
RAW_TAXI_PATH=NYC-taxi
RAW_WEATHER_PATH=Weather.csv

SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

Before running the pipeline, make sure:

- `luigi` and `python-dotenv` are installed in the Python environment used by `pipeline.py`
- `gcloud` is authenticated to the same GCP project as `GCP_PROJECT`
- SnowSQL is installed locally

Run the full pipeline from the repository root:

```bash
./pipeline.py
```

If the Spark stage already succeeded and you only want to retry the Snowflake stage, run:

```bash
./pipeline.py --keep-flags
```

---

# Authors
Hanson Yang, Yujin Feng, Jennifer Chen, Zihan Ye, Ruoxuan Cao, Rita He

MSBA Class of 2026 team 19

University of California, Los Angeles (UCLA)
