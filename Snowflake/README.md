# Snowflake Data Warehouse Layer

## Overview

This directory contains the SQL logic used to implement the **Snowflake data warehouse layer** for the NYC Taxi demand analysis project.

The Snowflake layer performs the following tasks:

- Creates the **star schema tables**
- Loads processed data from **Google Cloud Storage (GCS)**
- Ensures **idempotent pipeline execution**
- Prepares clean analytical tables for **Tableau dashboards**

All tables are created inside the database and schema configured in the SQL script:

```
NYCTAXI_DB.STAR_SCHEMA
```

---

# Data Pipeline Integration

The upstream Spark pipeline writes processed tables to Google Cloud Storage.

Example bucket:

```
gs://msba405nyctaxi/
```

Snowflake then ingests these tables using:

- **External Stage**
- **Storage Integration**
- **COPY INTO commands**

This architecture separates:

```
Spark → Data Processing
Snowflake → Data Warehouse / Analytics
```

---

# Storage Integration

A Snowflake **Storage Integration** is used to securely access Google Cloud Storage.

The provided SQL template replaces the bucket name dynamically before execution so the same script can be used with a different GCS bucket.

This allows Snowflake to read data from GCS without embedding credentials.

---

# External Stage

An external stage connects Snowflake to the GCS bucket.

The external stage points to the configured GCS bucket and uses CSV files with headers, which matches the Spark output written by `spark-job.py`.

This stage is used as the source for all data loading operations.

---

# Star Schema Design

The warehouse uses a **star schema** optimized for analytical queries.

## Fact Table

### FACT_TAXI_DEMAND

Stores aggregated taxi demand metrics.

| Column | Description |
|------|-------------|
| date_id | Trip date |
| location_id | Location dimension key |
| weather_id | Weather dimension key |
| trip_count | Number of taxi trips |
| avg_fare | Average fare amount |
| avg_tip | Average tip amount |
| avg_tip_pct | Average tip percentage |
| avg_total | Average total trip cost |
| avg_distance | Average trip distance |

---

## Dimension Tables

### DIM_LOCATION

Stores pickup location information.

| Column | Description |
|------|-------------|
| location_id | Unique location identifier |
| location_name | Location name (JFK / Central Park) |

---

### DIM_TIME

Stores calendar attributes derived upstream in Spark.

The current implementation loads all columns from the Spark-generated CSV:

```sql
DATE_ID
YEAR
QUARTER
MONTH
DAY
DAY_OF_WEEK
```

---

### DIM_WEATHER

Stores categorical weather conditions.

| Column | Description |
|------|-------------|
| weather_id | Weather dimension key |
| rain_level | Rain category |
| snow_level | Snow category |
| temp_level | Temperature category |

---

# Data Loading Strategy

Data is loaded into Snowflake using the **COPY INTO** command from the external stage.

Examples:

```sql
COPY INTO dim_location 
FROM @gcp_taxi_stage/dim_location/;
```

### DIM_TIME Special Handling

The current SQL does not use a special computed-column load path for `DIM_TIME`.  
It loads the Spark-generated CSV directly, the same way it loads the other dimension and fact tables.

---

# Pipeline Idempotency

The loading process is designed to be **idempotent**, meaning the pipeline can be safely re-run without introducing duplicate records.

Key design considerations include:

- deterministic dimension keys
- controlled data ingestion from GCS
- reproducible schema creation

---

# Data Validation Queries

Several validation queries are included to verify data integrity.

### Table Record Counts

```sql
SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL
SELECT 'fact_taxi_demand', COUNT(*) FROM fact_taxi_demand;
```

---

### Monthly Coverage Check

Ensures that all months appear correctly in the dataset.

```sql
SELECT 
    YEAR,
    MONTH,
    COUNT(*) AS records_per_month
FROM dim_time
GROUP BY 1,2
ORDER BY 1,2;
```

---

### Sample Analytical Query

Example join across fact and dimension tables:

```sql
SELECT  
    f.date_id,
    l.location_name,
    w.rain_level,
    f.trip_count,
    f.avg_total
FROM fact_taxi_demand f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_time t ON f.date_id = t.date_id
JOIN dim_weather w ON f.weather_id = w.weather_id
ORDER BY f.date_id DESC
LIMIT 10;
```

---

# Analytical Objective

The Snowflake warehouse enables analysis of the following hypothesis:

```
Weather conditions influence taxi demand in New York City.
```

The star schema allows efficient analysis of:

- taxi demand under different weather conditions
- location-based demand patterns
- fare behavior during adverse weather
- temporal demand trends

The warehouse tables are used as the data source for **Tableau dashboards**.
