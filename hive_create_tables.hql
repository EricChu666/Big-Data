--------------------------------------------------------------------------------
-- Hive DDL Script - Create External Tables
-- Purpose: Create Hive tables pointing to CSV data in HDFS
-- Usage: hive -f hive_create_tables.hql
--------------------------------------------------------------------------------

-- Create database
CREATE DATABASE IF NOT EXISTS fleet_analytics
COMMENT 'Fleet management and risk analytics database'
LOCATION '/user/hive/warehouse/fleet_data';

USE fleet_analytics;

--------------------------------------------------------------------------------
-- 1. TRUCKS TABLE
-- Contains monthly mileage and fuel consumption data (2009-2013)
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS trucks;

CREATE EXTERNAL TABLE trucks (
    driverid STRING,
    truckid STRING,
    model STRING,
    jun13_miles INT,
    jun13_gas INT,
    may13_miles INT,
    may13_gas INT,
    apr13_miles INT,
    apr13_gas INT,
    mar13_miles INT,
    mar13_gas INT,
    feb13_miles INT,
    feb13_gas INT,
    jan13_miles INT,
    jan13_gas INT,
    dec12_miles INT,
    dec12_gas INT,
    nov12_miles INT,
    nov12_gas INT,
    oct12_miles INT,
    oct12_gas INT,
    sep12_miles INT,
    sep12_gas INT,
    aug12_miles INT,
    aug12_gas INT,
    jul12_miles INT,
    jul12_gas INT,
    jun12_miles INT,
    jun12_gas INT,
    may12_miles INT,
    may12_gas INT,
    apr12_miles INT,
    apr12_gas INT,
    mar12_miles INT,
    mar12_gas INT,
    feb12_miles INT,
    feb12_gas INT,
    jan12_miles INT,
    jan12_gas INT,
    dec11_miles INT,
    dec11_gas INT,
    nov11_miles INT,
    nov11_gas INT,
    oct11_miles INT,
    oct11_gas INT,
    sep11_miles INT,
    sep11_gas INT,
    aug11_miles INT,
    aug11_gas INT,
    jul11_miles INT,
    jul11_gas INT,
    jun11_miles INT,
    jun11_gas INT,
    may11_miles INT,
    may11_gas INT,
    apr11_miles INT,
    apr11_gas INT,
    mar11_miles INT,
    mar11_gas INT,
    feb11_miles INT,
    feb11_gas INT,
    jan11_miles INT,
    jan11_gas INT,
    dec10_miles INT,
    dec10_gas INT,
    nov10_miles INT,
    nov10_gas INT,
    oct10_miles INT,
    oct10_gas INT,
    sep10_miles INT,
    sep10_gas INT,
    aug10_miles INT,
    aug10_gas INT,
    jul10_miles INT,
    jul10_gas INT,
    jun10_miles INT,
    jun10_gas INT,
    may10_miles INT,
    may10_gas INT,
    apr10_miles INT,
    apr10_gas INT,
    mar10_miles INT,
    mar10_gas INT,
    feb10_miles INT,
    feb10_gas INT,
    jan10_miles INT,
    jan10_gas INT,
    dec09_miles INT,
    dec09_gas INT,
    nov09_miles INT,
    nov09_gas INT,
    oct09_miles INT,
    oct09_gas INT,
    sep09_miles INT,
    sep09_gas INT,
    aug09_miles INT,
    aug09_gas INT,
    jul09_miles INT,
    jul09_gas INT,
    jun09_miles INT,
    jun09_gas INT,
    may09_miles INT,
    may09_gas INT,
    apr09_miles INT,
    apr09_gas INT,
    mar09_miles INT,
    mar09_gas INT,
    feb09_miles INT,
    feb09_gas INT,
    jan09_miles INT,
    jan09_gas INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/fleet_data/trucks'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------------------------------------------
-- 2. DRIVER_RISK TABLE
-- Contains risky event counts per driver
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS driver_risk;

CREATE EXTERNAL TABLE driver_risk (
    driverid STRING,
    risky_event_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/fleet_data/driver_risk'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------------------------------------------
-- 3. DRIVER_MPG TABLE
-- Contains average MPG per driver
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS driver_mpg;

CREATE EXTERNAL TABLE driver_mpg (
    driverid STRING,
    avg_mpg DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/fleet_data/driver_mpg'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------------------------------------------
-- 4. MODEL_RISK TABLE
-- Contains risky event counts per truck model
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS model_risk;

CREATE EXTERNAL TABLE model_risk (
    model STRING,
    risky_event_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/fleet_data/model_risk'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------------------------------------------
-- 5. CITY_RISK TABLE
-- Contains risky event counts per city
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS city_risk;

CREATE EXTERNAL TABLE city_risk (
    city STRING,
    risky_event_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/fleet_data/city_risk'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------------------------------------------
-- 6. GEOLOCATION TABLE
-- Contains detailed event data with location information
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS geolocation;

CREATE EXTERNAL TABLE geolocation (
    truckid STRING,
    driverid STRING,
    event STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    city STRING,
    state STRING,
    velocity INT,
    event_ind INT,
    idling_ind INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/fleet_data/geolocation'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------------------------------------------
-- 7. EVENT_TYPE_COUNTS TABLE
-- Contains aggregated counts by event type
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS event_type_counts;

CREATE EXTERNAL TABLE event_type_counts (
    event STRING,
    risky_event_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/fleet_data/event_type_counts'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------------------------------------------
-- 8. TRUCKS_MG TABLE
-- Contains truck-level MPG data
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS trucks_mg;

CREATE EXTERNAL TABLE trucks_mg (
    truckid STRING,
    avg_mpg DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/fleet_data/trucks_mg'
TBLPROPERTIES ("skip.header.line.count"="1");

--------------------------------------------------------------------------------
-- VERIFICATION QUERIES
--------------------------------------------------------------------------------

-- Check row counts
SELECT 'trucks' as table_name, COUNT(*) as row_count FROM trucks
UNION ALL
SELECT 'driver_risk', COUNT(*) FROM driver_risk
UNION ALL
SELECT 'driver_mpg', COUNT(*) FROM driver_mpg
UNION ALL
SELECT 'model_risk', COUNT(*) FROM model_risk
UNION ALL
SELECT 'city_risk', COUNT(*) FROM city_risk
UNION ALL
SELECT 'geolocation', COUNT(*) FROM geolocation
UNION ALL
SELECT 'event_type_counts', COUNT(*) FROM event_type_counts
UNION ALL
SELECT 'trucks_mg', COUNT(*) FROM trucks_mg;

-- Sample data from each table
SELECT 'Sample from trucks:' as info;
SELECT * FROM trucks LIMIT 3;

SELECT 'Sample from geolocation:' as info;
SELECT * FROM geolocation LIMIT 3;

SELECT 'Sample from driver_risk:' as info;
SELECT * FROM driver_risk LIMIT 3;

-- Show all tables
SHOW TABLES;
