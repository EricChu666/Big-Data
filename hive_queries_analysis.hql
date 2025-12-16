--------------------------------------------------------------------------------
-- Hive Analytical Queries
-- Purpose: Complex analytical queries for fleet insights
-- Usage: hive -f hive_queries_analysis.hql
--------------------------------------------------------------------------------

USE fleet_analytics;

--------------------------------------------------------------------------------
-- QUERY 1: Driver Performance Ranking
-- Combines risk and efficiency metrics
--------------------------------------------------------------------------------
SELECT 
    'QUERY 1: Driver Performance Ranking' as query_name;

SELECT 
    dr.driverid,
    t.model,
    dr.risky_event_count,
    dm.avg_mpg,
    RANK() OVER (ORDER BY dr.risky_event_count ASC, dm.avg_mpg DESC) as performance_rank
FROM driver_risk dr
JOIN driver_mpg dm ON dr.driverid = dm.driverid
JOIN trucks t ON dr.driverid = t.driverid
ORDER BY performance_rank
LIMIT 20;

--------------------------------------------------------------------------------
-- QUERY 2: Model Performance Analysis
-- Compare truck models across multiple dimensions
--------------------------------------------------------------------------------
SELECT 
    'QUERY 2: Model Performance Analysis' as query_name;

SELECT 
    t.model,
    COUNT(DISTINCT t.driverid) as driver_count,
    AVG(dr.risky_event_count) as avg_risk_events,
    AVG(dm.avg_mpg) as avg_fuel_efficiency,
    mr.risky_event_count as total_model_risk,
    CASE 
        WHEN AVG(dr.risky_event_count) > 5 THEN 'High Risk'
        WHEN AVG(dr.risky_event_count) > 3 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_category
FROM trucks t
LEFT JOIN driver_risk dr ON t.driverid = dr.driverid
LEFT JOIN driver_mpg dm ON t.driverid = dm.driverid
LEFT JOIN model_risk mr ON t.model = mr.model
GROUP BY t.model, mr.risky_event_count
ORDER BY avg_risk_events DESC;

--------------------------------------------------------------------------------
-- QUERY 3: Geographic Risk Hotspots
-- Identify cities with highest concentration of risky events
--------------------------------------------------------------------------------
SELECT 
    'QUERY 3: Geographic Risk Hotspots' as query_name;

SELECT 
    g.city,
    g.state,
    COUNT(*) as total_events,
    SUM(g.event_ind) as risky_events,
    ROUND(SUM(g.event_ind) * 100.0 / COUNT(*), 2) as risk_percentage,
    AVG(g.velocity) as avg_velocity,
    SUM(g.idling_ind) as idling_events,
    cr.risky_event_count as city_total_risk
FROM geolocation g
LEFT JOIN city_risk cr ON g.city = cr.city
GROUP BY g.city, g.state, cr.risky_event_count
HAVING SUM(g.event_ind) > 0
ORDER BY risky_events DESC
LIMIT 20;

--------------------------------------------------------------------------------
-- QUERY 4: Event Type Distribution by Driver
-- Analyze which drivers have which types of risky events
--------------------------------------------------------------------------------
SELECT 
    'QUERY 4: Event Type Distribution by Driver' as query_name;

SELECT 
    g.driverid,
    t.model,
    SUM(CASE WHEN g.event = 'overspeed' THEN 1 ELSE 0 END) as overspeed_count,
    SUM(CASE WHEN g.event = 'lane departure' THEN 1 ELSE 0 END) as lane_departure_count,
    SUM(CASE WHEN g.event = 'unsafe following distance' THEN 1 ELSE 0 END) as unsafe_following_count,
    SUM(CASE WHEN g.event = 'unsafe tail distance' THEN 1 ELSE 0 END) as unsafe_tail_count,
    COUNT(*) as total_risky_events,
    dr.risky_event_count as reported_risk_count
FROM geolocation g
JOIN trucks t ON g.driverid = t.driverid
JOIN driver_risk dr ON g.driverid = dr.driverid
WHERE g.event_ind = 1
GROUP BY g.driverid, t.model, dr.risky_event_count
ORDER BY total_risky_events DESC
LIMIT 20;

--------------------------------------------------------------------------------
-- QUERY 5: Fuel Efficiency vs Risk Correlation
-- Determine if safer drivers are more fuel efficient
--------------------------------------------------------------------------------
SELECT 
    'QUERY 5: Fuel Efficiency vs Risk Correlation' as query_name;

SELECT 
    CASE 
        WHEN dr.risky_event_count >= 7 THEN 'High Risk (7+)'
        WHEN dr.risky_event_count >= 4 THEN 'Medium Risk (4-6)'
        ELSE 'Low Risk (0-3)'
    END as risk_group,
    COUNT(*) as driver_count,
    ROUND(AVG(dm.avg_mpg), 2) as avg_mpg,
    ROUND(MIN(dm.avg_mpg), 2) as min_mpg,
    ROUND(MAX(dm.avg_mpg), 2) as max_mpg,
    ROUND(STDDEV(dm.avg_mpg), 2) as stddev_mpg
FROM driver_risk dr
JOIN driver_mpg dm ON dr.driverid = dm.driverid
GROUP BY 
    CASE 
        WHEN dr.risky_event_count >= 7 THEN 'High Risk (7+)'
        WHEN dr.risky_event_count >= 4 THEN 'Medium Risk (4-6)'
        ELSE 'Low Risk (0-3)'
    END
ORDER BY avg_mpg DESC;

--------------------------------------------------------------------------------
-- QUERY 6: Velocity Analysis by Event Type
-- Understand speed patterns during risky events
--------------------------------------------------------------------------------
SELECT 
    'QUERY 6: Velocity Analysis by Event Type' as query_name;

SELECT 
    g.event,
    COUNT(*) as event_count,
    ROUND(AVG(g.velocity), 2) as avg_velocity,
    ROUND(MIN(g.velocity), 2) as min_velocity,
    ROUND(MAX(g.velocity), 2) as max_velocity,
    PERCENTILE_APPROX(g.velocity, 0.5) as median_velocity,
    PERCENTILE_APPROX(g.velocity, 0.95) as p95_velocity
FROM geolocation g
WHERE g.event_ind = 1
GROUP BY g.event
ORDER BY avg_velocity DESC;

--------------------------------------------------------------------------------
-- QUERY 7: Idling Analysis
-- Identify drivers and locations with excessive idling
--------------------------------------------------------------------------------
SELECT 
    'QUERY 7: Idling Analysis' as query_name;

SELECT 
    g.driverid,
    t.model,
    g.city,
    COUNT(*) as total_observations,
    SUM(g.idling_ind) as idling_count,
    ROUND(SUM(g.idling_ind) * 100.0 / COUNT(*), 2) as idling_percentage,
    dr.risky_event_count
FROM geolocation g
JOIN trucks t ON g.driverid = t.driverid
JOIN driver_risk dr ON g.driverid = dr.driverid
GROUP BY g.driverid, t.model, g.city, dr.risky_event_count
HAVING SUM(g.idling_ind) > 5
ORDER BY idling_count DESC
LIMIT 20;

--------------------------------------------------------------------------------
-- QUERY 8: State-Level Risk Summary
-- Aggregate risk metrics by state
--------------------------------------------------------------------------------
SELECT 
    'QUERY 8: State-Level Risk Summary' as query_name;

SELECT 
    g.state,
    COUNT(DISTINCT g.driverid) as unique_drivers,
    COUNT(DISTINCT g.city) as unique_cities,
    COUNT(*) as total_events,
    SUM(g.event_ind) as risky_events,
    ROUND(SUM(g.event_ind) * 100.0 / COUNT(*), 2) as risk_percentage,
    ROUND(AVG(g.velocity), 2) as avg_velocity
FROM geolocation g
GROUP BY g.state
ORDER BY risky_events DESC;

--------------------------------------------------------------------------------
-- QUERY 9: Driver-Model Mismatch Analysis
-- Identify high-risk drivers in high-risk vehicles
--------------------------------------------------------------------------------
SELECT 
    'QUERY 9: Driver-Model Mismatch Analysis' as query_name;

SELECT 
    t.driverid,
    t.model,
    dr.risky_event_count as driver_risk,
    mr.risky_event_count as model_risk,
    (dr.risky_event_count + mr.risky_event_count) as combined_risk,
    dm.avg_mpg,
    CASE 
        WHEN dr.risky_event_count > 6 AND mr.risky_event_count > 50 THEN 'CRITICAL - Reassign Vehicle'
        WHEN dr.risky_event_count > 6 OR mr.risky_event_count > 50 THEN 'HIGH - Monitor Closely'
        ELSE 'ACCEPTABLE'
    END as recommendation
FROM trucks t
JOIN driver_risk dr ON t.driverid = dr.driverid
JOIN model_risk mr ON t.model = mr.model
JOIN driver_mpg dm ON t.driverid = dm.driverid
ORDER BY combined_risk DESC
LIMIT 20;

--------------------------------------------------------------------------------
-- QUERY 10: Event Frequency by Time Period (using geolocation as proxy)
-- Note: This assumes event records are somewhat chronological
--------------------------------------------------------------------------------
SELECT 
    'QUERY 10: Overall Fleet Statistics' as query_name;

SELECT 
    'Total Drivers' as metric,
    COUNT(DISTINCT driverid) as value
FROM trucks
UNION ALL
SELECT 
    'Total Risky Events',
    SUM(risky_event_count)
FROM driver_risk
UNION ALL
SELECT 
    'Average MPG',
    ROUND(AVG(avg_mpg), 2)
FROM driver_mpg
UNION ALL
SELECT 
    'Total Geolocation Events',
    COUNT(*)
FROM geolocation
UNION ALL
SELECT 
    'Risky Geolocation Events',
    SUM(event_ind)
FROM geolocation;

--------------------------------------------------------------------------------
-- END OF ANALYTICAL QUERIES
--------------------------------------------------------------------------------
