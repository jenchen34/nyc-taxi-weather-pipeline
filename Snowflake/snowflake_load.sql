CREATE DATABASE IF NOT EXISTS NYCTAXI_DB;
CREATE SCHEMA IF NOT EXISTS NYCTAXI_DB.STAR_SCHEMA;

-------------------------------------------------------------------------------------------
-- 1. Infrastructure setup
-------------------------------------------------------------------------------------------

CREATE OR REPLACE STORAGE INTEGRATION gcs_taxi_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://__GCS_BUCKET_NO_PREFIX__/');

DESCRIBE INTEGRATION gcs_taxi_int;

CREATE OR REPLACE STAGE gcp_taxi_stage
  URL = 'gcs://__GCS_BUCKET_NO_PREFIX__/'
  STORAGE_INTEGRATION = gcs_taxi_int
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

-------------------------------------------------------------------------------------------
-- 2. Star schema tables
-------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE NYCTAXI_DB.STAR_SCHEMA.DIM_LOCATION (
    LOCATION_ID INT PRIMARY KEY,
    LOCATION_NAME STRING
);

CREATE OR REPLACE TABLE NYCTAXI_DB.STAR_SCHEMA.DIM_TIME (
    DATE_ID DATE PRIMARY KEY,
    YEAR INT,
    QUARTER INT,
    MONTH INT,
    DAY INT,
    DAY_OF_WEEK INT
);

CREATE OR REPLACE TABLE NYCTAXI_DB.STAR_SCHEMA.DIM_WEATHER (
    RAIN_LEVEL STRING,
    SNOW_LEVEL STRING,
    TEMP_LEVEL STRING,
    WEATHER_ID INT PRIMARY KEY
);

CREATE OR REPLACE TABLE NYCTAXI_DB.STAR_SCHEMA.FACT_TAXI_DEMAND (
    DATE_ID DATE,
    LOCATION_ID INT,
    WEATHER_ID INT,
    TRIP_COUNT INT,
    AVG_FARE FLOAT,
    AVG_TIP FLOAT,
    AVG_TIP_PCT FLOAT,
    AVG_TOTAL FLOAT,
    AVG_DISTANCE FLOAT
);

-------------------------------------------------------------------------------------------
-- 3. Load processed outputs
-------------------------------------------------------------------------------------------

COPY INTO NYCTAXI_DB.STAR_SCHEMA.DIM_LOCATION
FROM @gcp_taxi_stage/dim_location/;

COPY INTO NYCTAXI_DB.STAR_SCHEMA.DIM_TIME
FROM @gcp_taxi_stage/dim_time/;

COPY INTO NYCTAXI_DB.STAR_SCHEMA.DIM_WEATHER
FROM @gcp_taxi_stage/dim_weather/;

COPY INTO NYCTAXI_DB.STAR_SCHEMA.FACT_TAXI_DEMAND
FROM @gcp_taxi_stage/fact_taxi_demand/;

-------------------------------------------------------------------------------------------
-- 4. Validation queries
-------------------------------------------------------------------------------------------

SELECT 'dim_location' AS table_name, COUNT(*) AS record_count
FROM NYCTAXI_DB.STAR_SCHEMA.DIM_LOCATION
UNION ALL
SELECT 'dim_time', COUNT(*)
FROM NYCTAXI_DB.STAR_SCHEMA.DIM_TIME
UNION ALL
SELECT 'dim_weather', COUNT(*)
FROM NYCTAXI_DB.STAR_SCHEMA.DIM_WEATHER
UNION ALL
SELECT 'fact_taxi_demand', COUNT(*)
FROM NYCTAXI_DB.STAR_SCHEMA.FACT_TAXI_DEMAND;

SELECT
    YEAR,
    MONTH,
    COUNT(*) AS records_per_month
FROM NYCTAXI_DB.STAR_SCHEMA.DIM_TIME
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT
    f.DATE_ID,
    l.LOCATION_NAME,
    w.RAIN_LEVEL,
    f.TRIP_COUNT,
    f.AVG_TOTAL
FROM NYCTAXI_DB.STAR_SCHEMA.FACT_TAXI_DEMAND f
JOIN NYCTAXI_DB.STAR_SCHEMA.DIM_LOCATION l
    ON f.LOCATION_ID = l.LOCATION_ID
JOIN NYCTAXI_DB.STAR_SCHEMA.DIM_TIME t
    ON f.DATE_ID = t.DATE_ID
JOIN NYCTAXI_DB.STAR_SCHEMA.DIM_WEATHER w
    ON f.WEATHER_ID = w.WEATHER_ID
ORDER BY f.DATE_ID DESC
LIMIT 10;

