--------------------------------------------------------------------------------
-- Hive Script: Time Series Data Preparation
-- Purpose: Create additional tables for time series analysis
-- Usage: hive -f hive_time_series_prep.hql
--------------------------------------------------------------------------------

USE fleet_analytics;

--------------------------------------------------------------------------------
-- Create unpivoted time series table from trucks data
-- This makes it easier to query and analyze trends
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS trucks_time_series;

CREATE TABLE trucks_time_series AS
SELECT driverid, truckid, model, '2009-01' as year_month, jan09_miles as miles, jan09_gas as gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-02', feb09_miles, feb09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-03', mar09_miles, mar09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-04', apr09_miles, apr09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-05', may09_miles, may09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-06', jun09_miles, jun09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-07', jul09_miles, jul09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-08', aug09_miles, aug09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-09', sep09_miles, sep09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-10', oct09_miles, oct09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-11', nov09_miles, nov09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2009-12', dec09_miles, dec09_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-01', jan10_miles, jan10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-02', feb10_miles, feb10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-03', mar10_miles, mar10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-04', apr10_miles, apr10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-05', may10_miles, may10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-06', jun10_miles, jun10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-07', jul10_miles, jul10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-08', aug10_miles, aug10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-09', sep10_miles, sep10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-10', oct10_miles, oct10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-11', nov10_miles, nov10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2010-12', dec10_miles, dec10_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-01', jan11_miles, jan11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-02', feb11_miles, feb11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-03', mar11_miles, mar11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-04', apr11_miles, apr11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-05', may11_miles, may11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-06', jun11_miles, jun11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-07', jul11_miles, jul11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-08', aug11_miles, aug11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-09', sep11_miles, sep11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-10', oct11_miles, oct11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-11', nov11_miles, nov11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2011-12', dec11_miles, dec11_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-01', jan12_miles, jan12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-02', feb12_miles, feb12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-03', mar12_miles, mar12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-04', apr12_miles, apr12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-05', may12_miles, may12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-06', jun12_miles, jun12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-07', jul12_miles, jul12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-08', aug12_miles, aug12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-09', sep12_miles, sep12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-10', oct12_miles, oct12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-11', nov12_miles, nov12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2012-12', dec12_miles, dec12_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2013-01', jan13_miles, jan13_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2013-02', feb13_miles, feb13_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2013-03', mar13_miles, mar13_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2013-04', apr13_miles, apr13_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2013-05', may13_miles, may13_gas FROM trucks
UNION ALL SELECT driverid, truckid, model, '2013-06', jun13_miles, jun13_gas FROM trucks;

-- Add calculated columns
ALTER TABLE trucks_time_series ADD COLUMNS (
    mpg DOUBLE,
    year INT,
    month INT
);

-- Update calculated fields (in Hive, we need to recreate the table)
DROP TABLE IF EXISTS trucks_time_series_final;

CREATE TABLE trucks_time_series_final AS
SELECT 
    driverid,
    truckid,
    model,
    year_month,
    miles,
    gas,
    CASE WHEN gas > 0 THEN miles / gas ELSE 0 END as mpg,
    CAST(SUBSTR(year_month, 1, 4) AS INT) as year,
    CAST(SUBSTR(year_month, 6, 2) AS INT) as month
FROM trucks_time_series
WHERE miles > 0 AND gas > 0;

-- Replace original table
DROP TABLE trucks_time_series;
ALTER TABLE trucks_time_series_final RENAME TO trucks_time_series;

SELECT 'Time series table created successfully' as status;
SELECT COUNT(*) as total_records FROM trucks_time_series;

--------------------------------------------------------------------------------
-- Query: Monthly Fleet Aggregates
--------------------------------------------------------------------------------
SELECT 
    year_month,
    COUNT(DISTINCT driverid) as active_drivers,
    SUM(miles) as total_miles,
    SUM(gas) as total_gas,
    ROUND(SUM(miles) / SUM(gas), 2) as fleet_mpg,
    ROUND(AVG(mpg), 2) as avg_driver_mpg,
    ROUND(MIN(mpg), 2) as min_mpg,
    ROUND(MAX(mpg), 2) as max_mpg
FROM trucks_time_series
GROUP BY year_month
ORDER BY year_month;

--------------------------------------------------------------------------------
-- Query: Yearly Trends
--------------------------------------------------------------------------------
SELECT 
    year,
    COUNT(DISTINCT driverid) as drivers,
    ROUND(AVG(mpg), 2) as avg_mpg,
    SUM(miles) as total_miles,
    SUM(gas) as total_gas
FROM trucks_time_series
GROUP BY year
ORDER BY year;

--------------------------------------------------------------------------------
-- Query: Best and Worst Performing Months
--------------------------------------------------------------------------------
SELECT 'Best Months' as category, year_month, ROUND(AVG(mpg), 2) as avg_mpg
FROM trucks_time_series
GROUP BY year_month
ORDER BY avg_mpg DESC
LIMIT 5
UNION ALL
SELECT 'Worst Months', year_month, ROUND(AVG(mpg), 2)
FROM trucks_time_series
GROUP BY year_month
ORDER BY avg_mpg ASC
LIMIT 5;
