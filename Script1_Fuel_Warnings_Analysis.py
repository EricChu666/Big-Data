"""
================================================================================
SCRIPT 1: FUEL WARNINGS ANALYSIS
================================================================================
Purpose: Generate fuel_warnings_tableau.csv from raw truck data
Input Files: 
    - trucks.csv (historical monthly miles and gas data)
    - driver_mpg.csv (current period totals)
Output File: 
    - fuel_warnings_tableau.csv

This script detects when a driver's current fuel efficiency (MPG) falls outside
their historical pattern using statistical Z-score analysis.
================================================================================
"""

import pandas as pd
import numpy as np

print("=" * 80)
print("SCRIPT 1: FUEL WARNINGS ANALYSIS")
print("Purpose: Detect fuel efficiency anomalies outside historical patterns")
print("=" * 80)

# ==============================================================================
# STEP 1: LOAD RAW DATA
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 1: LOAD RAW DATA")
print("=" * 80)

# Load historical truck data (54 months of miles and gas)
trucks = pd.read_csv('/mnt/user-data/uploads/trucks.csv')
print(f"\nLoaded trucks.csv:")
print(f"  - Rows: {len(trucks)}")
print(f"  - Columns: {len(trucks.columns)}")
print(f"  - Drivers: {trucks['driverid'].nunique()}")

# Load current period MPG data
driver_mpg = pd.read_csv('/mnt/user-data/uploads/driver_mpg.csv')
print(f"\nLoaded driver_mpg.csv:")
print(f"  - Rows: {len(driver_mpg)}")
print(f"  - Columns: {list(driver_mpg.columns)}")

# Display sample of current MPG data
print(f"\nSample of driver_mpg.csv:")
print(driver_mpg.head(3).to_string(index=False))

# ==============================================================================
# STEP 2: RESHAPE HISTORICAL DATA FROM WIDE TO LONG FORMAT
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 2: RESHAPE HISTORICAL DATA (Wide to Long Format)")
print("=" * 80)

# Identify all month columns
# Format: {month}{year}_miles and {month}{year}_gas (e.g., jun13_miles, jun13_gas)
month_cols = [col for col in trucks.columns if '_miles' in col]
months = [col.replace('_miles', '') for col in month_cols]

print(f"\nTime periods found: {len(months)} months")
print(f"Date range: {months[-1]} to {months[0]}")  # jan09 to jun13
print(f"Sample months: {months[:5]}...")

# Reshape data: create one row per driver per month
long_data = []

for _, row in trucks.iterrows():
    driverid = row['driverid']
    truckid = row['truckid']
    model = row['model']
    
    for month in months:
        miles_col = f'{month}_miles'
        gas_col = f'{month}_gas'
        
        miles = row[miles_col]
        gas = row[gas_col]
        
        # Calculate MPG for this month
        # Avoid division by zero
        if gas > 0:
            mpg = miles / gas
        else:
            mpg = np.nan
        
        long_data.append({
            'driverid': driverid,
            'truckid': truckid,
            'model': model,
            'month': month,
            'miles': miles,
            'gas': gas,
            'mpg': mpg
        })

historical_df = pd.DataFrame(long_data)

print(f"\nReshaped historical data:")
print(f"  - Total records: {len(historical_df):,}")
print(f"  - Records per driver: {len(historical_df) // trucks['driverid'].nunique()}")
print(f"\nSample of reshaped data:")
print(historical_df.head(5).to_string(index=False))

# ==============================================================================
# STEP 3: CALCULATE HISTORICAL STATISTICS PER DRIVER
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 3: CALCULATE HISTORICAL STATISTICS PER DRIVER")
print("=" * 80)

# For each driver, calculate statistics from their 54 monthly MPG values
driver_stats = historical_df.groupby('driverid').agg({
    'mpg': [
        'mean',      # Historical mean MPG
        'std',       # Historical standard deviation
        'min',       # Minimum MPG observed
        'max',       # Maximum MPG observed
        'median',    # Median MPG
        lambda x: x.quantile(0.25),  # 25th percentile (Q1)
        lambda x: x.quantile(0.75)   # 75th percentile (Q3)
    ]
}).round(4)

# Flatten column names
driver_stats.columns = ['hist_mean', 'hist_std', 'hist_min', 'hist_max', 
                        'hist_median', 'hist_q1', 'hist_q3']
driver_stats = driver_stats.reset_index()

# Calculate IQR (Interquartile Range) for alternative anomaly detection
driver_stats['hist_iqr'] = driver_stats['hist_q3'] - driver_stats['hist_q1']
driver_stats['iqr_lower'] = driver_stats['hist_q1'] - 1.5 * driver_stats['hist_iqr']
driver_stats['iqr_upper'] = driver_stats['hist_q3'] + 1.5 * driver_stats['hist_iqr']

print(f"\nDriver statistics calculated:")
print(f"  - Drivers: {len(driver_stats)}")
print(f"  - Columns: {list(driver_stats.columns)}")

print(f"\nSample driver statistics:")
print(driver_stats.head(3).to_string(index=False))

print(f"\nStatistics summary across all drivers:")
print(f"  - Average hist_mean: {driver_stats['hist_mean'].mean():.2f} MPG")
print(f"  - Average hist_std: {driver_stats['hist_std'].mean():.2f} MPG")

# ==============================================================================
# STEP 4: CALCULATE MODEL-LEVEL STATISTICS (Fleet Benchmarks)
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 4: CALCULATE MODEL-LEVEL STATISTICS (Fleet Benchmarks)")
print("=" * 80)

# Calculate average MPG and std dev for each truck model
model_stats = historical_df.groupby('model')['mpg'].agg(['mean', 'std']).round(4)
model_stats.columns = ['model_mean', 'model_std']
model_stats = model_stats.reset_index()

print(f"\nModel statistics:")
print(model_stats.to_string(index=False))

# ==============================================================================
# STEP 5: MERGE CURRENT DATA WITH HISTORICAL STATISTICS
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 5: MERGE CURRENT DATA WITH HISTORICAL STATISTICS")
print("=" * 80)

# Start with current period data
analysis_df = driver_mpg.copy()

print(f"\nStarting with driver_mpg: {len(analysis_df)} drivers")

# Merge with driver historical statistics
analysis_df = analysis_df.merge(driver_stats, on='driverid', how='left')
print(f"After merging driver_stats: {len(analysis_df)} drivers")

# Merge with model statistics
analysis_df = analysis_df.merge(model_stats, on='model', how='left')
print(f"After merging model_stats: {len(analysis_df)} drivers")

print(f"\nColumns after merge: {list(analysis_df.columns)}")

# ==============================================================================
# STEP 6: CALCULATE Z-SCORES
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 6: CALCULATE Z-SCORES")
print("=" * 80)

print("""
Z-Score Formula:
    z_score = (current_value - historical_mean) / historical_std_dev

Interpretation:
    - Z = 0: Current MPG equals historical average
    - Z > 0: Current MPG is HIGHER than historical average  
    - Z < 0: Current MPG is LOWER than historical average
    - |Z| > 2: Current MPG is significantly different (95% confidence)
""")

# Calculate Z-score comparing driver's current MPG to their own history
analysis_df['z_score_personal'] = (
    (analysis_df['avg_mpg'] - analysis_df['hist_mean']) / analysis_df['hist_std']
)

# Calculate Z-score comparing driver's current MPG to their truck model's fleet average
analysis_df['z_score_model'] = (
    (analysis_df['avg_mpg'] - analysis_df['model_mean']) / analysis_df['model_std']
)

# Calculate Z-score comparing to entire fleet
fleet_mean = historical_df['mpg'].mean()
fleet_std = historical_df['mpg'].std()
analysis_df['z_score_fleet'] = (
    (analysis_df['avg_mpg'] - fleet_mean) / fleet_std
)

print(f"\nZ-Score calculations completed:")
print(f"  - Fleet mean MPG: {fleet_mean:.2f}")
print(f"  - Fleet std MPG: {fleet_std:.2f}")

print(f"\nSample Z-scores:")
sample = analysis_df[['driverid', 'avg_mpg', 'hist_mean', 'hist_std', 'z_score_personal']].head(5)
print(sample.to_string(index=False))

# ==============================================================================
# STEP 7: CALCULATE PERCENTAGE DEVIATION
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 7: CALCULATE PERCENTAGE DEVIATION")
print("=" * 80)

print("""
Percentage Deviation Formula:
    pct_dev = ((current_mpg - historical_mean) / historical_mean) * 100

This tells us the percentage above or below the driver's historical average.
""")

# Percentage deviation from personal history
analysis_df['pct_dev_personal'] = (
    (analysis_df['avg_mpg'] - analysis_df['hist_mean']) / analysis_df['hist_mean']
) * 100

# Percentage deviation from model average
analysis_df['pct_dev_model'] = (
    (analysis_df['avg_mpg'] - analysis_df['model_mean']) / analysis_df['model_mean']
) * 100

print(f"Sample percentage deviations:")
sample = analysis_df[['driverid', 'avg_mpg', 'hist_mean', 'pct_dev_personal']].head(5)
print(sample.to_string(index=False))

# ==============================================================================
# STEP 8: CALCULATE CONTROL LIMITS
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 8: CALCULATE CONTROL LIMITS")
print("=" * 80)

print("""
Control Limits define the "normal" range for each driver:
    - Upper 2-sigma: hist_mean + 2 * hist_std (95% upper bound)
    - Lower 2-sigma: hist_mean - 2 * hist_std (95% lower bound)
    - Upper 1.5-sigma: hist_mean + 1.5 * hist_std (Watch threshold)
    - Lower 1.5-sigma: hist_mean - 1.5 * hist_std (Watch threshold)
""")

# 2-sigma control limits (95% confidence)
analysis_df['upper_2sigma'] = analysis_df['hist_mean'] + 2 * analysis_df['hist_std']
analysis_df['lower_2sigma'] = analysis_df['hist_mean'] - 2 * analysis_df['hist_std']

# 1.5-sigma control limits (for "Watch" threshold)
analysis_df['upper_1_5sigma'] = analysis_df['hist_mean'] + 1.5 * analysis_df['hist_std']
analysis_df['lower_1_5sigma'] = analysis_df['hist_mean'] - 1.5 * analysis_df['hist_std']

# Check if current MPG is outside IQR bounds
analysis_df['outside_iqr'] = (
    (analysis_df['avg_mpg'] < analysis_df['iqr_lower']) | 
    (analysis_df['avg_mpg'] > analysis_df['iqr_upper'])
)

print(f"Sample control limits:")
sample = analysis_df[['driverid', 'hist_mean', 'lower_2sigma', 'upper_2sigma', 'avg_mpg']].head(5)
print(sample.to_string(index=False))

# ==============================================================================
# STEP 9: ASSIGN ALERT LEVELS BASED ON Z-SCORE
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 9: ASSIGN ALERT LEVELS BASED ON Z-SCORE")
print("=" * 80)

print("""
Alert Level Thresholds (based on |z_score_personal|):
    |Z| >= 2.5  →  CRITICAL  (99% outside normal - extremely unusual)
    |Z| >= 2.0  →  WARNING   (95% outside normal - very unusual)
    |Z| >= 1.5  →  WATCH     (87% outside normal - unusual)
    |Z| >= 1.0  →  MONITOR   (68% outside normal - slightly unusual)
    |Z| <  1.0  →  NORMAL    (within normal variation)
""")

def get_alert_level(z_score):
    """
    Assign alert level based on absolute Z-score.
    """
    abs_z = abs(z_score)
    
    if abs_z >= 2.5:
        return 'CRITICAL'
    elif abs_z >= 2.0:
        return 'WARNING'
    elif abs_z >= 1.5:
        return 'WATCH'
    elif abs_z >= 1.0:
        return 'MONITOR'
    else:
        return 'NORMAL'

# Apply alert level function
analysis_df['alert_level'] = analysis_df['z_score_personal'].apply(get_alert_level)

# Determine direction (HIGH or LOW)
def get_direction(z_score):
    """
    Determine if MPG is higher or lower than historical average.
    """
    if z_score < -1.0:
        return 'LOW'
    elif z_score > 1.0:
        return 'HIGH'
    else:
        return 'NORMAL'

analysis_df['direction'] = analysis_df['z_score_personal'].apply(get_direction)

# Create combined alert description
analysis_df['alert_full'] = analysis_df['direction'] + ' - ' + analysis_df['alert_level']

# Flag drivers needing attention (alert level is not NORMAL)
analysis_df['needs_attention'] = analysis_df['alert_level'] != 'NORMAL'

print(f"\nAlert Level Distribution:")
print(analysis_df['alert_level'].value_counts().sort_index())

print(f"\nDirection Distribution:")
print(analysis_df['direction'].value_counts())

# ==============================================================================
# STEP 10: DISPLAY RESULTS
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 10: ANALYSIS RESULTS")
print("=" * 80)

print(f"\nTotal Drivers Analyzed: {len(analysis_df)}")
print(f"Drivers with Alerts (needs_attention=True): {analysis_df['needs_attention'].sum()}")

# Show drivers with alerts
alert_drivers = analysis_df[analysis_df['needs_attention']].sort_values('z_score_personal')

print(f"\n--- DRIVERS WITH FUEL ALERTS ---")
for _, row in alert_drivers.iterrows():
    print(f"\nDriver {row['driverid']} ({row['model']}):")
    print(f"  Current MPG: {row['avg_mpg']:.2f}")
    print(f"  Historical Mean: {row['hist_mean']:.2f} ± {row['hist_std']:.2f}")
    print(f"  Z-Score: {row['z_score_personal']:.2f}")
    print(f"  Deviation: {row['pct_dev_personal']:.1f}%")
    print(f"  Alert Level: {row['alert_level']}")
    print(f"  Direction: {row['direction']}")

# ==============================================================================
# STEP 11: EXPORT TO CSV
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 11: EXPORT TO fuel_warnings_tableau.csv")
print("=" * 80)

# Select columns for export
export_columns = [
    'driverid', 'truckid', 'model', 'total_miles', 'total_gas', 'avg_mpg',
    'hist_mean', 'hist_std', 'hist_min', 'hist_max', 'hist_median', 'hist_q1', 'hist_q3',
    'model_mean', 'model_std',
    'z_score_personal', 'z_score_model', 'z_score_fleet',
    'pct_dev_personal', 'pct_dev_model',
    'upper_2sigma', 'lower_2sigma', 'upper_1_5sigma', 'lower_1_5sigma',
    'iqr_lower', 'iqr_upper', 'outside_iqr',
    'alert_level', 'direction', 'alert_full', 'needs_attention'
]

# Export to CSV
output_path = '/mnt/user-data/outputs/fuel_warnings_tableau.csv'
analysis_df[export_columns].to_csv(output_path, index=False)

print(f"\nExported: {output_path}")
print(f"  - Rows: {len(analysis_df)}")
print(f"  - Columns: {len(export_columns)}")

print(f"\nColumn descriptions:")
column_descriptions = {
    'driverid': 'Driver identifier',
    'truckid': 'Truck identifier',
    'model': 'Truck model/make',
    'total_miles': 'Total miles driven in current period',
    'total_gas': 'Total gallons of gas used in current period',
    'avg_mpg': 'Current average MPG (total_miles / total_gas)',
    'hist_mean': 'Historical average MPG (mean of 54 months)',
    'hist_std': 'Historical standard deviation of MPG',
    'hist_min': 'Minimum historical MPG',
    'hist_max': 'Maximum historical MPG',
    'hist_median': 'Median historical MPG',
    'hist_q1': '25th percentile of historical MPG',
    'hist_q3': '75th percentile of historical MPG',
    'model_mean': 'Fleet average MPG for this truck model',
    'model_std': 'Fleet standard deviation for this truck model',
    'z_score_personal': 'Z-score vs personal history',
    'z_score_model': 'Z-score vs model fleet average',
    'z_score_fleet': 'Z-score vs entire fleet average',
    'pct_dev_personal': 'Percentage deviation from personal history',
    'pct_dev_model': 'Percentage deviation from model average',
    'upper_2sigma': 'Upper control limit (mean + 2*std)',
    'lower_2sigma': 'Lower control limit (mean - 2*std)',
    'upper_1_5sigma': 'Upper watch limit (mean + 1.5*std)',
    'lower_1_5sigma': 'Lower watch limit (mean - 1.5*std)',
    'iqr_lower': 'Lower IQR bound (Q1 - 1.5*IQR)',
    'iqr_upper': 'Upper IQR bound (Q3 + 1.5*IQR)',
    'outside_iqr': 'True if current MPG is outside IQR bounds',
    'alert_level': 'Alert classification (NORMAL/MONITOR/WATCH/WARNING/CRITICAL)',
    'direction': 'Direction of deviation (NORMAL/HIGH/LOW)',
    'alert_full': 'Combined direction and alert level',
    'needs_attention': 'True if alert_level is not NORMAL'
}

for col in export_columns[:10]:  # Show first 10 descriptions
    print(f"  - {col}: {column_descriptions.get(col, 'N/A')}")
print("  ... (see documentation for full list)")

print("\n" + "=" * 80)
print("SCRIPT 1 COMPLETE: fuel_warnings_tableau.csv generated successfully")
print("=" * 80)
