"""
PySpark Script: Multi-Dimensional Risk Scoring
Purpose: Create comprehensive risk scores combining driver behavior, vehicle model, 
         route, and temporal factors
Author: Big Data Analytics Team
Usage: spark-submit pyspark_risk_scoring.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Fleet Risk Scoring System") \
    .enableHiveSupport() \
    .getOrCreate()

print("=" * 80)
print("FLEET RISK SCORING SYSTEM")
print("=" * 80)

# ============================================================================
# STEP 1: Load Data from Hive Tables
# ============================================================================
print("\n[1/6] Loading data from Hive tables...")

# Load all tables
driver_risk = spark.sql("SELECT * FROM fleet_analytics.driver_risk")
driver_mpg = spark.sql("SELECT * FROM fleet_analytics.driver_mpg")
model_risk = spark.sql("SELECT * FROM fleet_analytics.model_risk")
city_risk = spark.sql("SELECT * FROM fleet_analytics.city_risk")
geolocation = spark.sql("SELECT * FROM fleet_analytics.geolocation")
trucks = spark.sql("SELECT * FROM fleet_analytics.trucks")

print(f"✓ Loaded {driver_risk.count()} driver risk records")
print(f"✓ Loaded {geolocation.count()} geolocation events")
print(f"✓ Loaded {trucks.count()} truck records")

# ============================================================================
# STEP 2: Calculate Driver Behavior Score (0-100, lower is better)
# ============================================================================
print("\n[2/6] Calculating driver behavior scores...")

# Normalize risky event count to 0-100 scale
max_driver_risk = driver_risk.agg(max("risky_event_count")).collect()[0][0]

driver_behavior_score = driver_risk.withColumn(
    "behavior_score",
    (col("risky_event_count") / lit(max_driver_risk) * 100).cast("int")
).select("driverid", "behavior_score", "risky_event_count")

driver_behavior_score.show(10)

# ============================================================================
# STEP 3: Calculate Fuel Efficiency Score (0-100, lower is better)
# ============================================================================
print("\n[3/6] Calculating fuel efficiency scores...")

# Inverse MPG score (lower MPG = higher score/worse)
max_mpg = driver_mpg.agg(max("avg_mpg")).collect()[0][0]
min_mpg = driver_mpg.agg(min("avg_mpg")).collect()[0][0]

fuel_efficiency_score = driver_mpg.withColumn(
    "efficiency_score",
    ((lit(max_mpg) - col("avg_mpg")) / (lit(max_mpg) - lit(min_mpg)) * 100).cast("int")
).select("driverid", "efficiency_score", "avg_mpg")

fuel_efficiency_score.show(10)

# ============================================================================
# STEP 4: Calculate Geographic Risk Score
# ============================================================================
print("\n[4/6] Calculating geographic risk scores...")

# Count risky events per driver by location
geo_risk_by_driver = geolocation.filter(col("event_ind") == 1) \
    .groupBy("driverid") \
    .agg(
        count("*").alias("geo_risky_events"),
        countDistinct("city").alias("risky_cities_count"),
        avg("velocity").alias("avg_velocity_during_events")
    )

# Normalize geographic risk
max_geo_risk = geo_risk_by_driver.agg(max("geo_risky_events")).collect()[0][0]

geo_risk_score = geo_risk_by_driver.withColumn(
    "geo_risk_score",
    (col("geo_risky_events") / lit(max_geo_risk) * 100).cast("int")
).select("driverid", "geo_risk_score", "geo_risky_events", "risky_cities_count")

geo_risk_score.show(10)

# ============================================================================
# STEP 5: Calculate Vehicle Model Risk Score
# ============================================================================
print("\n[5/6] Calculating vehicle model risk scores...")

# Join trucks with model_risk to get model risk for each driver
max_model_risk = model_risk.agg(max("risky_event_count")).collect()[0][0]

vehicle_risk = trucks.select("driverid", "model") \
    .join(model_risk, "model", "left") \
    .withColumn(
        "vehicle_risk_score",
        (col("risky_event_count") / lit(max_model_risk) * 100).cast("int")
    ).select("driverid", "model", "vehicle_risk_score")

vehicle_risk.show(10)

# ============================================================================
# STEP 6: Calculate Composite Risk Score
# ============================================================================
print("\n[6/6] Calculating composite risk scores...")

# Join all scores
composite_risk = driver_behavior_score \
    .join(fuel_efficiency_score, "driverid", "left") \
    .join(geo_risk_score, "driverid", "left") \
    .join(vehicle_risk, "driverid", "left")

# Fill nulls with 0 for drivers without geographic events
composite_risk = composite_risk.fillna(0, subset=["geo_risk_score", "geo_risky_events", "risky_cities_count"])

# Calculate weighted composite score
# Weights: Behavior (40%), Efficiency (20%), Geographic (25%), Vehicle (15%)
composite_risk = composite_risk.withColumn(
    "composite_risk_score",
    (
        col("behavior_score") * 0.40 +
        col("efficiency_score") * 0.20 +
        col("geo_risk_score") * 0.25 +
        col("vehicle_risk_score") * 0.15
    ).cast("int")
)

# Add risk category
composite_risk = composite_risk.withColumn(
    "risk_category",
    when(col("composite_risk_score") >= 70, "HIGH RISK")
    .when(col("composite_risk_score") >= 40, "MEDIUM RISK")
    .otherwise("LOW RISK")
)

# ============================================================================
# RESULTS
# ============================================================================
print("\n" + "=" * 80)
print("RISK SCORING RESULTS")
print("=" * 80)

# Show top 10 highest risk drivers
print("\nTOP 10 HIGHEST RISK DRIVERS:")
composite_risk.orderBy(col("composite_risk_score").desc()).show(10, truncate=False)

# Show top 10 lowest risk drivers
print("\nTOP 10 LOWEST RISK DRIVERS:")
composite_risk.orderBy(col("composite_risk_score").asc()).show(10, truncate=False)

# Risk category distribution
print("\nRISK CATEGORY DISTRIBUTION:")
composite_risk.groupBy("risk_category") \
    .agg(count("*").alias("driver_count")) \
    .orderBy("risk_category") \
    .show()

# Statistics
print("\nRISK SCORE STATISTICS:")
composite_risk.select(
    avg("composite_risk_score").alias("avg_risk"),
    min("composite_risk_score").alias("min_risk"),
    max("composite_risk_score").alias("max_risk"),
    stddev("composite_risk_score").alias("stddev_risk")
).show()

# ============================================================================
# SAVE RESULTS
# ============================================================================
print("\n[SAVING] Writing results to HDFS...")

# Save to HDFS
output_path = "/user/output/risk_scores"
composite_risk.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

print(f"✓ Results saved to: {output_path}")

# Save to Hive table for easy querying
composite_risk.write.mode("overwrite").saveAsTable("fleet_analytics.driver_risk_scores")
print("✓ Results saved to Hive table: fleet_analytics.driver_risk_scores")

# ============================================================================
# RECOMMENDATIONS
# ============================================================================
print("\n" + "=" * 80)
print("BUSINESS RECOMMENDATIONS")
print("=" * 80)

high_risk_drivers = composite_risk.filter(col("risk_category") == "HIGH RISK").count()
medium_risk_drivers = composite_risk.filter(col("risk_category") == "MEDIUM RISK").count()
low_risk_drivers = composite_risk.filter(col("risk_category") == "LOW RISK").count()

print(f"\n1. IMMEDIATE ACTION REQUIRED:")
print(f"   - {high_risk_drivers} drivers are HIGH RISK - require immediate intervention")
print(f"   - Recommend safety training and vehicle reassignment")

print(f"\n2. MONITORING NEEDED:")
print(f"   - {medium_risk_drivers} drivers are MEDIUM RISK - require monitoring")
print(f"   - Implement monthly performance reviews")

print(f"\n3. BEST PRACTICES:")
print(f"   - {low_risk_drivers} drivers are LOW RISK - identify best practices")
print(f"   - Use as mentors for training programs")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)

# Stop Spark session
spark.stop()
