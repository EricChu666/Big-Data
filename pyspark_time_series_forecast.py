"""
PySpark Script: Time Series Forecasting for Fleet Analytics
Purpose: Predict future fuel consumption and risky events using historical patterns
Data Sources: trucks.csv (monthly miles/gas 2009-2013), trucks_mg.csv (date-based MPG)
Author: Big Data Analytics Team
Usage: spark-submit pyspark_time_series_forecast.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Fleet Time Series Forecasting") \
    .config("spark.sql.adaptive.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

print("=" * 80)
print("FLEET TIME SERIES FORECASTING SYSTEM")
print("Predicting Future Fuel Consumption and Risk Patterns")
print("=" * 80)

# ============================================================================
# STEP 1: Load and Transform trucks.csv Data
# ============================================================================
print("\n[1/7] Loading and transforming trucks.csv time series data...")

# Load trucks table
trucks = spark.sql("SELECT * FROM fleet_analytics.trucks")
print(f"✓ Loaded {trucks.count()} truck records")

# Transform wide format to long format (unpivot)
# Create list of all month-year columns
months = [
    ('jan09', '2009-01'), ('feb09', '2009-02'), ('mar09', '2009-03'), ('apr09', '2009-04'),
    ('may09', '2009-05'), ('jun09', '2009-06'), ('jul09', '2009-07'), ('aug09', '2009-08'),
    ('sep09', '2009-09'), ('oct09', '2009-10'), ('nov09', '2009-11'), ('dec09', '2009-12'),
    ('jan10', '2010-01'), ('feb10', '2010-02'), ('mar10', '2010-03'), ('apr10', '2010-04'),
    ('may10', '2010-05'), ('jun10', '2010-06'), ('jul10', '2010-07'), ('aug10', '2010-08'),
    ('sep10', '2010-09'), ('oct10', '2010-10'), ('nov10', '2010-11'), ('dec10', '2010-12'),
    ('jan11', '2011-01'), ('feb11', '2011-02'), ('mar11', '2011-03'), ('apr11', '2011-04'),
    ('may11', '2011-05'), ('jun11', '2011-06'), ('jul11', '2011-07'), ('aug11', '2011-08'),
    ('sep11', '2011-09'), ('oct11', '2011-10'), ('nov11', '2011-11'), ('dec11', '2011-12'),
    ('jan12', '2012-01'), ('feb12', '2012-02'), ('mar12', '2012-03'), ('apr12', '2012-04'),
    ('may12', '2012-05'), ('jun12', '2012-06'), ('jul12', '2012-07'), ('aug12', '2012-08'),
    ('sep12', '2012-09'), ('oct12', '2012-10'), ('nov12', '2012-11'), ('dec12', '2012-12'),
    ('jan13', '2013-01'), ('feb13', '2013-02'), ('mar13', '2013-03'), ('apr13', '2013-04'),
    ('may13', '2013-05'), ('jun13', '2013-06')
]

# Create time series dataframe
time_series_data = None

for month_code, year_month in months:
    month_df = trucks.select(
        col('driverid'),
        col('truckid'),
        col('model'),
        lit(year_month).alias('year_month'),
        col(f'{month_code}_miles').alias('miles'),
        col(f'{month_code}_gas').alias('gas')
    )
    
    if time_series_data is None:
        time_series_data = month_df
    else:
        time_series_data = time_series_data.union(month_df)

# Calculate MPG and add time features
time_series_data = time_series_data \
    .withColumn('mpg', when(col('gas') > 0, col('miles') / col('gas')).otherwise(0)) \
    .withColumn('date', to_date(concat(col('year_month'), lit('-01')))) \
    .withColumn('year', year(col('date'))) \
    .withColumn('month', month(col('date'))) \
    .withColumn('quarter', quarter(col('date'))) \
    .filter((col('miles') > 0) & (col('gas') > 0))  # Remove invalid records

print(f"✓ Transformed to {time_series_data.count()} monthly records")
time_series_data.show(10)

# ============================================================================
# STEP 2: Calculate Aggregate Fleet Metrics
# ============================================================================
print("\n[2/7] Calculating aggregate fleet metrics...")

# Aggregate by month
fleet_monthly = time_series_data.groupBy('year_month', 'date', 'year', 'month', 'quarter') \
    .agg(
        sum('miles').alias('total_miles'),
        sum('gas').alias('total_gas'),
        avg('mpg').alias('avg_mpg'),
        count('driverid').alias('active_drivers'),
        stddev('mpg').alias('mpg_stddev')
    ) \
    .withColumn('fleet_mpg', col('total_miles') / col('total_gas')) \
    .orderBy('date')

print("✓ Fleet monthly aggregates:")
fleet_monthly.show(10)

# ============================================================================
# STEP 3: Add Lag Features for Time Series Prediction
# ============================================================================
print("\n[3/7] Creating lag features for prediction...")

# Create window for lag features
window_spec = Window.orderBy('date')

# Add lag features (previous 1, 2, 3 months)
fleet_features = fleet_monthly \
    .withColumn('mpg_lag1', lag('fleet_mpg', 1).over(window_spec)) \
    .withColumn('mpg_lag2', lag('fleet_mpg', 2).over(window_spec)) \
    .withColumn('mpg_lag3', lag('fleet_mpg', 3).over(window_spec)) \
    .withColumn('miles_lag1', lag('total_miles', 1).over(window_spec)) \
    .withColumn('gas_lag1', lag('total_gas', 1).over(window_spec)) \
    .withColumn('month_num', col('month').cast('double')) \
    .withColumn('quarter_num', col('quarter').cast('double')) \
    .dropna()  # Remove rows with null lag values

print(f"✓ Created lag features, {fleet_features.count()} records available for training")
fleet_features.show(10)

# ============================================================================
# STEP 4: Prepare Training and Test Sets
# ============================================================================
print("\n[4/7] Preparing training and test datasets...")

# Split: Use 2009-2012 for training, 2013 for testing
train_data = fleet_features.filter(col('year') < 2013)
test_data = fleet_features.filter(col('year') == 2013)

print(f"✓ Training set: {train_data.count()} months (2009-2012)")
print(f"✓ Test set: {test_data.count()} months (2013)")

# Assemble features
feature_cols = ['mpg_lag1', 'mpg_lag2', 'mpg_lag3', 'miles_lag1', 'gas_lag1', 
                'month_num', 'quarter_num', 'active_drivers']

assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')

train_assembled = assembler.transform(train_data)
test_assembled = assembler.transform(test_data)

# ============================================================================
# STEP 5: Train Linear Regression Model
# ============================================================================
print("\n[5/7] Training Linear Regression model...")

lr = LinearRegression(
    featuresCol='features',
    labelCol='fleet_mpg',
    predictionCol='predicted_mpg',
    maxIter=100,
    regParam=0.1,
    elasticNetParam=0.5
)

lr_model = lr.fit(train_assembled)

print(f"✓ Model trained successfully")
print(f"  Coefficients: {lr_model.coefficients}")
print(f"  Intercept: {lr_model.intercept}")
print(f"  RMSE on training: {lr_model.summary.rootMeanSquaredError:.4f}")
print(f"  R²: {lr_model.summary.r2:.4f}")

# ============================================================================
# STEP 6: Make Predictions and Evaluate
# ============================================================================
print("\n[6/7] Making predictions on test set (2013)...")

predictions = lr_model.transform(test_assembled)

# Calculate evaluation metrics
evaluator_rmse = RegressionEvaluator(
    labelCol='fleet_mpg',
    predictionCol='predicted_mpg',
    metricName='rmse'
)

evaluator_r2 = RegressionEvaluator(
    labelCol='fleet_mpg',
    predictionCol='predicted_mpg',
    metricName='r2'
)

evaluator_mae = RegressionEvaluator(
    labelCol='fleet_mpg',
    predictionCol='predicted_mpg',
    metricName='mae'
)

rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)
mae = evaluator_mae.evaluate(predictions)

print("\n" + "=" * 80)
print("MODEL PERFORMANCE METRICS")
print("=" * 80)
print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")
print(f"R² Score: {r2:.4f}")
print(f"Mean Absolute Error (MAE): {mae:.4f}")

# Show predictions vs actual
print("\nPREDICTIONS vs ACTUAL (2013):")
predictions.select(
    'year_month',
    'fleet_mpg',
    'predicted_mpg',
    (col('fleet_mpg') - col('predicted_mpg')).alias('error'),
    'total_miles',
    'total_gas'
).orderBy('year_month').show(20, truncate=False)

# ============================================================================
# STEP 7: Forecast Future Months (Jul-Dec 2013)
# ============================================================================
print("\n[7/7] Forecasting future months (Jul-Dec 2013)...")

# Get last known values from June 2013
last_record = fleet_features.filter(col('year_month') == '2013-06').first()

if last_record:
    # Create forecast for next 6 months
    forecast_months = [
        ('2013-07', 7, 3), ('2013-08', 8, 3), ('2013-09', 9, 3),
        ('2013-10', 10, 4), ('2013-11', 11, 4), ('2013-12', 12, 4)
    ]
    
    forecast_data = []
    
    # Use last 3 months as initial lags
    mpg_lag1 = last_record['fleet_mpg']
    mpg_lag2 = last_record['mpg_lag1']
    mpg_lag3 = last_record['mpg_lag2']
    miles_lag1 = last_record['total_miles']
    gas_lag1 = last_record['total_gas']
    active_drivers = last_record['active_drivers']
    
    for year_month, month_num, quarter_num in forecast_months:
        # Create feature vector
        forecast_row = spark.createDataFrame([
            (year_month, mpg_lag1, mpg_lag2, mpg_lag3, miles_lag1, gas_lag1, 
             float(month_num), float(quarter_num), active_drivers)
        ], ['year_month', 'mpg_lag1', 'mpg_lag2', 'mpg_lag3', 'miles_lag1', 
            'gas_lag1', 'month_num', 'quarter_num', 'active_drivers'])
        
        # Assemble and predict
        forecast_assembled = assembler.transform(forecast_row)
        forecast_pred = lr_model.transform(forecast_assembled)
        
        predicted_mpg = forecast_pred.select('predicted_mpg').first()[0]
        
        # Store forecast
        forecast_data.append((year_month, predicted_mpg))
        
        # Update lags for next iteration
        mpg_lag3 = mpg_lag2
        mpg_lag2 = mpg_lag1
        mpg_lag1 = predicted_mpg
    
    # Create forecast dataframe
    forecast_df = spark.createDataFrame(forecast_data, ['year_month', 'forecasted_mpg'])
    
    print("\nFORECASTED FLEET MPG (Jul-Dec 2013):")
    forecast_df.show(truncate=False)
else:
    print("⚠ Warning: Could not find June 2013 data for forecasting")
    forecast_df = None

# ============================================================================
# STEP 8: Analyze Trends
# ============================================================================
print("\n" + "=" * 80)
print("TREND ANALYSIS")
print("=" * 80)

# Calculate year-over-year trends
yearly_trends = fleet_monthly.groupBy('year') \
    .agg(
        avg('fleet_mpg').alias('avg_yearly_mpg'),
        sum('total_miles').alias('total_yearly_miles'),
        sum('total_gas').alias('total_yearly_gas')
    ) \
    .orderBy('year')

print("\nYEAR-OVER-YEAR TRENDS:")
yearly_trends.show()

# Calculate seasonal patterns
seasonal_patterns = fleet_monthly.groupBy('quarter') \
    .agg(
        avg('fleet_mpg').alias('avg_quarterly_mpg'),
        avg('total_miles').alias('avg_quarterly_miles')
    ) \
    .orderBy('quarter')

print("\nSEASONAL PATTERNS (by Quarter):")
seasonal_patterns.show()

# ============================================================================
# SAVE RESULTS
# ============================================================================
print("\n[SAVING] Writing results to HDFS and Hive...")

# Save predictions
predictions.select('year_month', 'fleet_mpg', 'predicted_mpg', 'total_miles', 'total_gas') \
    .coalesce(1) \
    .write.mode('overwrite') \
    .csv('/user/output/fuel_predictions', header=True)

print("✓ Predictions saved to: /user/output/fuel_predictions")

# Save forecast
if forecast_df:
    forecast_df.coalesce(1) \
        .write.mode('overwrite') \
        .csv('/user/output/fuel_forecast', header=True)
    print("✓ Forecast saved to: /user/output/fuel_forecast")

# Save to Hive tables
predictions.select('year_month', 'fleet_mpg', 'predicted_mpg', 'total_miles', 'total_gas') \
    .write.mode('overwrite') \
    .saveAsTable('fleet_analytics.fuel_predictions')

if forecast_df:
    forecast_df.write.mode('overwrite').saveAsTable('fleet_analytics.fuel_forecast')

print("✓ Results saved to Hive tables")

# ============================================================================
# BUSINESS INSIGHTS
# ============================================================================
print("\n" + "=" * 80)
print("BUSINESS INSIGHTS & RECOMMENDATIONS")
print("=" * 80)

# Calculate improvement/decline
first_year_mpg = yearly_trends.filter(col('year') == 2009).select('avg_yearly_mpg').first()[0]
last_year_mpg = yearly_trends.filter(col('year') == 2013).select('avg_yearly_mpg').first()[0]
mpg_change = ((last_year_mpg - first_year_mpg) / first_year_mpg) * 100

print(f"\n1. OVERALL TREND (2009-2013):")
print(f"   - Starting MPG (2009): {first_year_mpg:.2f}")
print(f"   - Current MPG (2013): {last_year_mpg:.2f}")
print(f"   - Change: {mpg_change:+.2f}%")

if mpg_change > 0:
    print(f"   ✓ Fleet efficiency has IMPROVED")
else:
    print(f"   ⚠ Fleet efficiency has DECLINED - investigate causes")

print(f"\n2. PREDICTION ACCURACY:")
print(f"   - Model R² Score: {r2:.4f} ({'Good' if r2 > 0.7 else 'Needs Improvement'})")
print(f"   - Average Prediction Error: ±{mae:.2f} MPG")

print(f"\n3. FORECASTING:")
if forecast_df:
    avg_forecast = forecast_df.agg(avg('forecasted_mpg')).first()[0]
    print(f"   - Expected MPG (Jul-Dec 2013): {avg_forecast:.2f}")
    print(f"   - Trend: {'Improving' if avg_forecast > last_year_mpg else 'Declining'}")

print(f"\n4. RECOMMENDATIONS:")
print(f"   - Monitor actual vs predicted MPG monthly")
print(f"   - Investigate months with large prediction errors")
print(f"   - Use forecasts for fuel budget planning")
print(f"   - Retrain model quarterly with new data")

print("\n" + "=" * 80)
print("FORECASTING COMPLETE")
print("=" * 80)

# Stop Spark
spark.stop()
