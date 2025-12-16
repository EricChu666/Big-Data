"""
PySpark Script: Time Series Forecasting (Standalone Version)
Purpose: Predict future fuel consumption - RUNS WITHOUT HADOOP/HIVE
Data Sources: CSV files directly
Usage: python pyspark_time_series_forecast_standalone.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import os

# Initialize Spark Session (Local mode - no Hadoop needed)
spark = SparkSession.builder \
    .appName("Fleet Time Series Forecasting - Standalone") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("=" * 80)
print("FLEET TIME SERIES FORECASTING SYSTEM (Standalone)")
print("Running in local mode - No Hadoop required!")
print("=" * 80)

# Set data directory
DATA_DIR = r"C:\UT Dallas\Class\2025 Fall\BUAN 6346.001 - Big Data\Project"

# ============================================================================
# STEP 1: Load CSV Data Directly
# ============================================================================
print("\n[1/7] Loading CSV data directly...")

# Load trucks.csv
trucks = spark.read.csv(
    os.path.join(DATA_DIR, "trucks.csv"),
    header=True,
    inferSchema=True
)

print(f"✓ Loaded {trucks.count()} truck records from CSV")

# ============================================================================
# STEP 2: Transform Wide Format to Time Series
# ============================================================================
print("\n[2/7] Transforming wide format to time series...")

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
    .filter((col('miles') > 0) & (col('gas') > 0))

print(f"✓ Transformed to {time_series_data.count()} monthly records")
time_series_data.show(10)

# ============================================================================
# STEP 3: Calculate Aggregate Fleet Metrics
# ============================================================================
print("\n[3/7] Calculating aggregate fleet metrics...")

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
# STEP 4: Add Lag Features
# ============================================================================
print("\n[4/7] Creating lag features for prediction...")

window_spec = Window.orderBy('date')

fleet_features = fleet_monthly \
    .withColumn('mpg_lag1', lag('fleet_mpg', 1).over(window_spec)) \
    .withColumn('mpg_lag2', lag('fleet_mpg', 2).over(window_spec)) \
    .withColumn('mpg_lag3', lag('fleet_mpg', 3).over(window_spec)) \
    .withColumn('miles_lag1', lag('total_miles', 1).over(window_spec)) \
    .withColumn('gas_lag1', lag('total_gas', 1).over(window_spec)) \
    .withColumn('month_num', col('month').cast('double')) \
    .withColumn('quarter_num', col('quarter').cast('double')) \
    .dropna()

print(f"✓ Created lag features, {fleet_features.count()} records available")
fleet_features.show(10)

# ============================================================================
# STEP 5: Prepare Training and Test Sets
# ============================================================================
print("\n[5/7] Preparing training and test datasets...")

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
# STEP 6: Train Linear Regression Model
# ============================================================================
print("\n[6/7] Training Linear Regression model...")

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
print(f"  RMSE on training: {lr_model.summary.rootMeanSquaredError:.4f}")
print(f"  R²: {lr_model.summary.r2:.4f}")

# ============================================================================
# STEP 7: Make Predictions and Evaluate
# ============================================================================
print("\n[7/7] Making predictions on test set (2013)...")

predictions = lr_model.transform(test_assembled)

# Calculate metrics
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
# STEP 8: Forecast Future Months
# ============================================================================
print("\n[8/8] Forecasting future months (Jul-Dec 2013)...")

last_record = fleet_features.filter(col('year_month') == '2013-06').first()

if last_record:
    forecast_months = [
        ('2013-07', 7, 3), ('2013-08', 8, 3), ('2013-09', 9, 3),
        ('2013-10', 10, 4), ('2013-11', 11, 4), ('2013-12', 12, 4)
    ]
    
    forecast_data = []
    
    mpg_lag1 = last_record['fleet_mpg']
    mpg_lag2 = last_record['mpg_lag1']
    mpg_lag3 = last_record['mpg_lag2']
    miles_lag1 = last_record['total_miles']
    gas_lag1 = last_record['total_gas']
    active_drivers = last_record['active_drivers']
    
    for year_month, month_num, quarter_num in forecast_months:
        forecast_row = spark.createDataFrame([
            (year_month, mpg_lag1, mpg_lag2, mpg_lag3, miles_lag1, gas_lag1, 
             float(month_num), float(quarter_num), active_drivers)
        ], ['year_month', 'mpg_lag1', 'mpg_lag2', 'mpg_lag3', 'miles_lag1', 
            'gas_lag1', 'month_num', 'quarter_num', 'active_drivers'])
        
        forecast_assembled = assembler.transform(forecast_row)
        forecast_pred = lr_model.transform(forecast_assembled)
        
        predicted_mpg = forecast_pred.select('predicted_mpg').first()[0]
        
        forecast_data.append((year_month, predicted_mpg))
        
        mpg_lag3 = mpg_lag2
        mpg_lag2 = mpg_lag1
        mpg_lag1 = predicted_mpg
    
    forecast_df = spark.createDataFrame(forecast_data, ['year_month', 'forecasted_mpg'])
    
    print("\nFORECASTED FLEET MPG (Jul-Dec 2013):")
    forecast_df.show(truncate=False)

# ============================================================================
# STEP 9: Trend Analysis
# ============================================================================
print("\n" + "=" * 80)
print("TREND ANALYSIS")
print("=" * 80)

yearly_trends = fleet_monthly.groupBy('year') \
    .agg(
        avg('fleet_mpg').alias('avg_yearly_mpg'),
        sum('total_miles').alias('total_yearly_miles'),
        sum('total_gas').alias('total_yearly_gas')
    ) \
    .orderBy('year')

print("\nYEAR-OVER-YEAR TRENDS:")
yearly_trends.show()

seasonal_patterns = fleet_monthly.groupBy('quarter') \
    .agg(
        avg('fleet_mpg').alias('avg_quarterly_mpg'),
        avg('total_miles').alias('avg_quarterly_miles')
    ) \
    .orderBy('quarter')

print("\nSEASONAL PATTERNS (by Quarter):")
seasonal_patterns.show()

# ============================================================================
# SAVE RESULTS TO CSV
# ============================================================================
print("\n[SAVING] Writing results to CSV files...")

output_dir = os.path.join(DATA_DIR, "output")
os.makedirs(output_dir, exist_ok=True)

# Save predictions
predictions.select('year_month', 'fleet_mpg', 'predicted_mpg', 'total_miles', 'total_gas') \
    .coalesce(1) \
    .write.mode('overwrite') \
    .csv(os.path.join(output_dir, 'fuel_predictions'), header=True)

print(f"✓ Predictions saved to: {output_dir}\\fuel_predictions")

# Save forecast
if forecast_df:
    forecast_df.coalesce(1) \
        .write.mode('overwrite') \
        .csv(os.path.join(output_dir, 'fuel_forecast'), header=True)
    print(f"✓ Forecast saved to: {output_dir}\\fuel_forecast")

# Save trends
yearly_trends.coalesce(1) \
    .write.mode('overwrite') \
    .csv(os.path.join(output_dir, 'yearly_trends'), header=True)

print(f"✓ Trends saved to: {output_dir}\\yearly_trends")

# ============================================================================
# BUSINESS INSIGHTS
# ============================================================================
print("\n" + "=" * 80)
print("BUSINESS INSIGHTS & RECOMMENDATIONS")
print("=" * 80)

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
    print(f"   ⚠ Fleet efficiency has DECLINED")

print(f"\n2. PREDICTION ACCURACY:")
print(f"   - Model R² Score: {r2:.4f} ({'Good' if r2 > 0.7 else 'Needs Improvement'})")
print(f"   - Average Prediction Error: ±{mae:.2f} MPG")

print(f"\n3. RECOMMENDATIONS:")
print(f"   - Monitor actual vs predicted MPG monthly")
print(f"   - Use forecasts for fuel budget planning")
print(f"   - Retrain model quarterly with new data")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE - Results saved to output folder")
print("=" * 80)

# Stop Spark
spark.stop()
