USE ROLE ACCOUNTADMIN;

---> set Warehouse Context
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

---> set the Database
CREATE DATABASE IF NOT EXISTS NYCTAXI_DB;
USE DATABASE NYCTAXI_DB;

---> set the Schema
CREATE SCHEMA IF NOT EXISTS STAR_SCHEMA;
USE SCHEMA STAR_SCHEMA;

-------------------------------------------------------------------------------------------
    -- Step 2: Create Table
        -- CREATE TABLE: https://docs.snowflake.com/en/sql-reference/sql/create-table
-------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------
---> 1. 基础设施配置 (Storage & Stage)
-------------------------------------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION gcs_taxi_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://msba405nyctaxi/');

DESCRIBE INTEGRATION gcs_taxi_int;

CREATE OR REPLACE STAGE gcp_taxi_stage
  URL = 'gcs://msba405nyctaxi/'
  STORAGE_INTEGRATION = gcs_taxi_int
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

-------------------------------------------------------------------------------------------
---> 2. 创建 Star Schema 表结构
-------------------------------------------------------------------------------------------

-- 地点维度
CREATE OR REPLACE TABLE dim_location (
   location_id INT PRIMARY KEY,
   location_name STRING
);

-- 时间维度 (核心修复：使用计算列，无视 CSV 里的错误月份)
CREATE OR REPLACE TABLE NYCTAXI_DB.STAR_SCHEMA.DIM_TIME (
    DATE_ID DATE PRIMARY KEY,
    YEAR INT AS (YEAR(DATE_ID)),           -- 自动计算，100%正确
    MONTH INT AS (MONTH(DATE_ID)),         -- 自动计算，解决1月和4月的问题
    DAY INT AS (DAY(DATE_ID)),             -- 自动计算
    DAY_OF_WEEK STRING AS (DAYNAME(DATE_ID)), 
    EXTRA_COL STRING                        -- 用来垫背 CSV 剩下的列
);

-- 天气维度
CREATE OR REPLACE TABLE dim_weather (
    rain_level STRING,
    snow_level STRING,
    temp_level STRING,
    weather_id INT PRIMARY KEY
);

-- 需求事实表
CREATE OR REPLACE TABLE fact_taxi_demand (
    date_id DATE,            
    location_id INT,
    weather_id INT,
    trip_count INT,          
    avg_fare FLOAT,
    avg_tip FLOAT,          
    avg_tip_pct FLOAT,       
    avg_total FLOAT,         
    avg_distance FLOAT
);

-------------------------------------------------------------------------------------------
---> 3. 执行数据导入 (针对性优化)
-------------------------------------------------------------------------------------------

COPY INTO dim_location FROM @gcp_taxi_stage/dim_location/;

-- 核心修复：只从 CSV 抽取第一列 DATE_ID，其他列让表结构自动生成
COPY INTO dim_time (DATE_ID) 
FROM (SELECT t.$1 FROM @gcp_taxi_stage/dim_time/ t);

COPY INTO dim_weather FROM @gcp_taxi_stage/dim_weather/;
COPY INTO fact_taxi_demand FROM @gcp_taxi_stage/fact_taxi_demand/;


-- 1. 证明维度表和事实表行数对齐
SELECT 'dim_location' as table_name, COUNT(*) as record_count FROM dim_location
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL
SELECT 'fact_taxi_demand', COUNT(*) FROM fact_taxi_demand;

-- 2. 证明 12 个月数据全在 (这是你之前 Tableau 出错的地方)
SELECT 
    YEAR, 
    MONTH, 
    COUNT(*) as Records_Per_Month
FROM dim_time
GROUP BY 1, 2
ORDER BY 1, 2;


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

