# Spark Data Pipeline

## Overview

This Spark pipeline processes NYC yellow taxi trip data together with daily weather observations to analyze how weather conditions influence taxi demand in New York City.

The pipeline reads raw taxi trip records and weather data from Google Cloud Storage (GCS), performs data cleaning and transformation using PySpark, and produces a **star schema** consisting of one fact table and three dimension tables. These outputs are used for downstream analytics and visualization.

---

# Data Sources

## Taxi Data

**Example location**

```
gs://msba405nyctaxi/NYC-taxi/
```

**Format**

Parquet

**Description**

Yellow taxi trip records stored as monthly parquet files from 2023–2025.

**Key Fields**

```
tpep_pickup_datetime
pulocationid
trip_distance
fare_amount
tip_amount
total_amount
```

---

## Weather Data

**Example location**

```
gs://msba405nyctaxi/Weather.csv
```

**Format**

CSV

**Key Fields**

```
date
PRCP   (precipitation)
SNOW   (snowfall)
TAVG   (average temperature)
```

Weather observations may contain multiple stations per day. The pipeline aggregates these records to create a single daily weather record.

---

# Pipeline Architecture

The Spark job performs the following ETL workflow:

```
Taxi parquet files (GCS)
        ↓
Schema standardization
        ↓
Taxi data cleaning & filtering
        ↓
Location filtering (JFK, Central Park)
        ↓
Weather aggregation by date
        ↓
Taxi + weather join
        ↓
Fact table aggregation
        ↓
Dimension table creation
        ↓
Write outputs to GCS
```

---

# Data Cleaning

Taxi trip records are filtered to remove invalid or extreme values.

Examples of filters applied:

```
1 ≤ passenger_count ≤ 6
0 < trip_distance ≤ 100
0 ≤ fare_amount ≤ 500
0 ≤ total_amount ≤ 500
```

Trips with missing pickup or dropoff timestamps are removed.

To simplify analysis, the pipeline only keeps trips originating from the following pickup locations:

```
JFK Airport        (location_id = 132)
Central Park       (location_id = 43)
```

---

# Weather Transformation

Weather observations are aggregated to daily values:

```
groupBy(date)
    max(PRCP)
    max(SNOW)
    avg(TAVG)
```

Continuous weather variables are converted into categorical buckets.

## Rain Levels

```
0 → no rain
0–0.1 → light rain
0.1–0.3 → moderate rain
>0.3 → heavy rain
```

## Snow Levels

```
0 → no snow
0–1 → light snow
1–3 → moderate snow
>3 → heavy snow
```

## Temperature Levels

```
<32 → freezing
32–50 → cold
50–70 → mild
>70 → warm
```

---

# Star Schema Output

The pipeline generates a star schema designed for analytical queries.

## Fact Table

`fact_taxi_demand`

Measures aggregated by:

```
date
location
weather conditions
```

Columns:

```
location_id
weather_id
trip_count
avg_fare
avg_tip
avg_tip_pct
avg_total
avg_distance
```

---

## Dimension Tables

### dim_time

```
date
year
quarter
month
day
day_of_week
```

---

### dim_location

```
location_id
location_name
```

Locations included:

```
JFK
Central Park
```

---

### dim_weather

```
weather_id
rain_level
snow_level
temp_level
```

---

# Output Location

All tables are written to Google Cloud Storage. With the current code, the exact bucket and folder names are controlled by environment variables passed from the root pipeline entrypoint.

Example output locations:

```
gs://msba405nyctaxi/fact_taxi_demand/
gs://msba405nyctaxi/dim_time/
gs://msba405nyctaxi/dim_weather/
gs://msba405nyctaxi/dim_location/
```

Outputs are written as CSV files with headers.

---

# Running the Pipeline

The Spark job is executed on a Dataproc Spark cluster.

In this repository, the intended way to run the Spark stage is through the root pipeline entrypoint, which submits `spark-job.py` to Dataproc and passes the configured GCS paths as arguments.

Example:

```
./pipeline.py
```

Running `spark-submit spark-job.py` locally is not the primary documented flow for this project.

---

# Analytical Objective

The purpose of this pipeline is to test the following hypothesis:

```
Weather conditions influence taxi demand in New York City.
```

The resulting dataset enables analysis of:

- taxi demand under different weather conditions  
- differences between airport and city pickup locations  
- fare and tipping behavior under adverse weather  

These outputs are used to create analytical dashboards in Tableau.
