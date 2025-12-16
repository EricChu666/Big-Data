"""
================================================================================
SCRIPT 2: COMPREHENSIVE RISK FACTOR ANALYSIS
================================================================================
Purpose: Generate comprehensive_risk_tableau.csv combining fuel efficiency 
         and driver risk data
Input Files: 
    - driver_risk.csv (risky event counts per driver)
    - driver_mpg.csv (current period MPG data)
    - trucks.csv (historical data for fuel analysis)
    - model_risk.csv (risky events by truck model)
    - city_risk.csv (risky events by city)
    - geolocation.csv (driver locations and events)
    - fuel_warnings_tableau.csv (output from Script 1)
Output File: 
    - comprehensive_risk_tableau.csv

This script calculates a Risk Factor (scale 1-10) for each driver and
identifies "Dual Concern" drivers who have both high risk AND fuel anomalies.
================================================================================
"""

import pandas as pd
import numpy as np

print("=" * 80)
print("SCRIPT 2: COMPREHENSIVE RISK FACTOR ANALYSIS")
print("Purpose: Calculate Risk Factor (1-10) and identify high-risk drivers")
print("=" * 80)

# ==============================================================================
# STEP 1: LOAD ALL DATA SOURCES
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 1: LOAD ALL DATA SOURCES")
print("=" * 80)

# Load driver risk data (risky event counts)
driver_risk = pd.read_csv('/mnt/user-data/uploads/driver_risk.csv')
print(f"\n1. driver_risk.csv:")
print(f"   - Rows: {len(driver_risk)}")
print(f"   - Columns: {list(driver_risk.columns)}")
print(f"   - Sample:")
print(driver_risk.head(3).to_string(index=False))

# Load fuel warnings data (from Script 1)
fuel_warnings = pd.read_csv('/mnt/user-data/uploads/fuel_warnings_tableau.csv')
print(f"\n2. fuel_warnings_tableau.csv:")
print(f"   - Rows: {len(fuel_warnings)}")
print(f"   - Key columns: driverid, model, avg_mpg, hist_mean, z_score_personal, alert_level")

# Load driver MPG data
driver_mpg = pd.read_csv('/mnt/user-data/uploads/driver_mpg.csv')
print(f"\n3. driver_mpg.csv:")
print(f"   - Rows: {len(driver_mpg)}")
print(f"   - Columns: {list(driver_mpg.columns)}")

# Load model risk data
model_risk = pd.read_csv('/mnt/user-data/uploads/model_risk.csv')
print(f"\n4. model_risk.csv:")
print(f"   - Rows: {len(model_risk)}")
print(f"   - Sample:")
print(model_risk.to_string(index=False))

# Load city risk data
city_risk = pd.read_csv('/mnt/user-data/uploads/city_risk.csv')
print(f"\n5. city_risk.csv:")
print(f"   - Rows: {len(city_risk)}")
print(f"   - Columns: {list(city_risk.columns)}")

# Load geolocation data
geolocation = pd.read_csv('/mnt/user-data/uploads/geolocation.csv')
print(f"\n6. geolocation.csv:")
print(f"   - Rows: {len(geolocation)}")
print(f"   - Event types: {geolocation['event'].unique()}")

# ==============================================================================
# STEP 2: EXTRACT DRIVER LOCATION INFORMATION
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 2: EXTRACT DRIVER LOCATION INFORMATION")
print("=" * 80)

# Get each driver's primary city (most frequent location)
driver_city = geolocation.groupby('driverid')['city'].agg(
    lambda x: x.value_counts().index[0]
).reset_index(name='primary_city')

print(f"\nPrimary city extracted for {len(driver_city)} drivers")
print(f"Sample:")
print(driver_city.head(5).to_string(index=False))

# Get each driver's last known location (latitude, longitude, city, state)
driver_location = geolocation.groupby('driverid').last()[
    ['latitude', 'longitude', 'city', 'state']
].reset_index()
driver_location.columns = ['driverid', 'last_latitude', 'last_longitude', 
                            'last_city', 'last_state']

print(f"\nLast known location extracted for {len(driver_location)} drivers")

# ==============================================================================
# STEP 3: MERGE ALL DATA SOURCES
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 3: MERGE ALL DATA SOURCES")
print("=" * 80)

# Start with driver_risk as the base
combined = driver_risk.copy()
combined.columns = ['driverid', 'risky_event_count']
print(f"\nStarting with driver_risk: {len(combined)} drivers")

# Merge with fuel warnings (contains model, avg_mpg, z_score, alert_level, etc.)
combined = combined.merge(
    fuel_warnings[['driverid', 'model', 'avg_mpg', 'hist_mean', 'z_score_personal', 
                   'pct_dev_personal', 'alert_level', 'needs_attention']],
    on='driverid',
    how='left'
)
print(f"After merging fuel_warnings: {len(combined)} drivers")

# Merge with driver_mpg for total_miles and total_gas
combined = combined.merge(
    driver_mpg[['driverid', 'total_miles', 'total_gas']],
    on='driverid',
    how='left'
)
print(f"After merging driver_mpg: {len(combined)} drivers")

# Merge with model_risk (rename column to avoid confusion)
combined = combined.merge(
    model_risk.rename(columns={'risky_event_count': 'model_risky_events'}),
    on='model',
    how='left'
)
print(f"After merging model_risk: {len(combined)} drivers")

# Merge with driver_location
combined = combined.merge(driver_location, on='driverid', how='left')
print(f"After merging driver_location: {len(combined)} drivers")

# Merge with driver_city
combined = combined.merge(driver_city, on='driverid', how='left')
print(f"After merging driver_city: {len(combined)} drivers")

# Merge with city_risk
combined = combined.merge(
    city_risk[['city', 'risky_event_count']].rename(
        columns={'city': 'primary_city', 'risky_event_count': 'city_risky_events'}
    ),
    on='primary_city',
    how='left'
)
print(f"After merging city_risk: {len(combined)} drivers")

# Fill NaN values for city_risky_events
combined['city_risky_events'] = combined['city_risky_events'].fillna(0)

print(f"\nFinal merged dataset: {len(combined)} drivers")
print(f"Columns: {list(combined.columns)}")

# ==============================================================================
# STEP 4: CALCULATE BASE EVENT RISK (Component 1 of Risk Factor)
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 4: CALCULATE BASE EVENT RISK")
print("=" * 80)

print("""
Base Event Risk Formula:
    base_event_risk = 1 + (risky_event_count / max_risky_event_count) * 7

This scales the risky event count to a range of 1-8:
    - 0 events  → 1.0 points
    - 7 events  → 4.5 points (midpoint)
    - 14 events → 8.0 points (maximum)

Why this formula?
    - Minimum of 1 ensures all drivers have a base risk
    - Maximum of 8 leaves room for fuel_penalty (0-2) and model_penalty (0-1)
    - Total possible: 8 + 2 + 1 = 11, capped at 10
""")

# Find the maximum risky event count
max_events = combined['risky_event_count'].max()
print(f"\nMaximum risky events in fleet: {max_events} (Driver with most events)")

# Calculate base event risk
combined['base_event_risk'] = 1 + (combined['risky_event_count'] / max_events) * 7

print(f"\nBase Event Risk calculation examples:")
examples = combined.nlargest(5, 'risky_event_count')[
    ['driverid', 'risky_event_count', 'base_event_risk']
]
print(examples.to_string(index=False))

print(f"\nManual verification for top driver:")
print(f"  risky_event_count = {max_events}")
print(f"  base_event_risk = 1 + ({max_events} / {max_events}) * 7 = 1 + 7 = 8.0")

# ==============================================================================
# STEP 5: CALCULATE FUEL PENALTY (Component 2 of Risk Factor)
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 5: CALCULATE FUEL PENALTY")
print("=" * 80)

print("""
Fuel Penalty Formula:
    fuel_penalty = |z_score_personal|, capped at maximum of 2.0

This adds 0-2 points based on how anomalous the fuel efficiency is:
    - |Z| = 0   → 0 points (exactly at historical average)
    - |Z| = 1.0 → 1.0 points (1 std dev from average)
    - |Z| = 1.5 → 1.5 points (1.5 std dev - WATCH level)
    - |Z| ≥ 2.0 → 2.0 points (capped - WARNING/CRITICAL level)

Why use absolute value?
    - Both HIGH and LOW deviations indicate anomalies
    - High MPG could mean fraud (reporting more miles than driven)
    - Low MPG could mean poor driving or vehicle issues
""")

# Calculate fuel penalty (absolute z-score, capped at 2)
combined['fuel_penalty'] = np.abs(combined['z_score_personal']).clip(upper=2.0)

# Fill NaN values with 0 (for drivers missing fuel data)
combined['fuel_penalty'] = combined['fuel_penalty'].fillna(0)

print(f"\nFuel Penalty calculation examples:")
examples = combined.nlargest(5, 'fuel_penalty')[
    ['driverid', 'z_score_personal', 'fuel_penalty']
]
print(examples.to_string(index=False))

print(f"\nManual verification for driver with highest |z_score|:")
max_z_driver = combined.loc[combined['z_score_personal'].abs().idxmax()]
print(f"  Driver: {max_z_driver['driverid']}")
print(f"  z_score_personal = {max_z_driver['z_score_personal']:.4f}")
print(f"  |z_score| = {abs(max_z_driver['z_score_personal']):.4f}")
print(f"  fuel_penalty = min({abs(max_z_driver['z_score_personal']):.4f}, 2.0) = {max_z_driver['fuel_penalty']:.4f}")

# ==============================================================================
# STEP 6: CALCULATE MODEL PENALTY (Component 3 of Risk Factor)
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 6: CALCULATE MODEL PENALTY")
print("=" * 80)

print("""
Model Penalty Formula:
    model_penalty = model_risky_events / max_model_risky_events

This adds 0-1 points based on the truck model's overall fleet risk:
    - Accounts for inherent risk differences between truck models
    - Ford (89 events) → 1.0 points
    - Crane (11 events) → 0.12 points

Why include model risk?
    - Some truck models may be more prone to incidents
    - Helps identify if the truck itself is a risk factor
""")

# Find maximum model events
max_model_events = combined['model_risky_events'].max()
print(f"\nMaximum model risky events: {max_model_events} (Ford)")

# Calculate model penalty
combined['model_penalty'] = combined['model_risky_events'] / max_model_events

# Fill NaN values with 0
combined['model_penalty'] = combined['model_penalty'].fillna(0)

print(f"\nModel Penalty by truck model:")
model_penalties = combined.groupby('model').agg({
    'model_risky_events': 'first',
    'model_penalty': 'first'
}).sort_values('model_penalty', ascending=False)
print(model_penalties.to_string())

# ==============================================================================
# STEP 7: CALCULATE FINAL RISK FACTOR
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 7: CALCULATE FINAL RISK FACTOR")
print("=" * 80)

print("""
Risk Factor Formula:
    risk_factor = base_event_risk + fuel_penalty + model_penalty

Then clipped to range [1, 10]:
    risk_factor = MIN(MAX(risk_factor, 1), 10)

Component breakdown:
    - base_event_risk: 1-8 points (from risky events)
    - fuel_penalty: 0-2 points (from fuel anomaly)
    - model_penalty: 0-1 points (from truck model risk)
    - Total possible: up to 11 points, capped at 10
""")

# Calculate raw risk factor
combined['risk_factor_raw'] = (
    combined['base_event_risk'] + 
    combined['fuel_penalty'] + 
    combined['model_penalty']
)

# Clip to 1-10 scale
combined['risk_factor'] = combined['risk_factor_raw'].clip(lower=1, upper=10).round(2)

print(f"\nRisk Factor calculation for top 5 riskiest drivers:")
top_5 = combined.nlargest(5, 'risk_factor')[
    ['driverid', 'risky_event_count', 'base_event_risk', 
     'fuel_penalty', 'model_penalty', 'risk_factor_raw', 'risk_factor']
]
print(top_5.to_string(index=False))

print(f"\nManual verification for Driver A97 (highest risk):")
a97 = combined[combined['driverid'] == 'A97'].iloc[0]
print(f"  base_event_risk = {a97['base_event_risk']:.2f}")
print(f"  fuel_penalty = {a97['fuel_penalty']:.2f}")
print(f"  model_penalty = {a97['model_penalty']:.2f}")
print(f"  risk_factor_raw = {a97['base_event_risk']:.2f} + {a97['fuel_penalty']:.2f} + {a97['model_penalty']:.2f} = {a97['risk_factor_raw']:.2f}")
print(f"  risk_factor (clipped) = min(max({a97['risk_factor_raw']:.2f}, 1), 10) = {a97['risk_factor']:.2f}")

# ==============================================================================
# STEP 8: CLASSIFY RISK LEVELS
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 8: CLASSIFY RISK LEVELS")
print("=" * 80)

print("""
Risk Level Classification:
    risk_factor >= 7.0  →  HIGH RISK   (requires immediate attention)
    risk_factor >= 5.0  →  MEDIUM RISK (monitor closely)
    risk_factor <  5.0  →  LOW RISK    (normal operations)

The threshold of 7.0 comes from the business requirement:
"Identify drivers with risk factor greater than or equal to 7.0"
""")

def classify_risk(risk_factor):
    """
    Classify driver into risk categories based on risk factor.
    """
    if risk_factor >= 7.0:
        return 'HIGH RISK'
    elif risk_factor >= 5.0:
        return 'MEDIUM RISK'
    else:
        return 'LOW RISK'

combined['risk_level'] = combined['risk_factor'].apply(classify_risk)

print(f"\nRisk Level Distribution:")
risk_dist = combined['risk_level'].value_counts().sort_index()
for level, count in risk_dist.items():
    pct = count / len(combined) * 100
    print(f"  {level}: {count} drivers ({pct:.1f}%)")

# ==============================================================================
# STEP 9: CREATE FLAG COLUMNS
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 9: CREATE FLAG COLUMNS")
print("=" * 80)

print("""
Flag Columns:

1. high_risk_flag:
   TRUE if risk_factor >= 7.0
   This answers: "Which drivers have risk >= 7.0?"

2. fuel_alert:
   TRUE if alert_level is NOT 'NORMAL'
   (i.e., alert_level IN ['MONITOR', 'WATCH', 'WARNING', 'CRITICAL'])
   This answers: "Which drivers have fuel anomalies?"

3. dual_concern:
   TRUE if BOTH high_risk_flag AND fuel_alert are TRUE
   This answers: "Which high-risk drivers ALSO have fuel problems?"
""")

# High risk flag (risk factor >= 7.0)
combined['high_risk_flag'] = combined['risk_factor'] >= 7.0

print(f"\nhigh_risk_flag:")
print(f"  Formula: risk_factor >= 7.0")
print(f"  TRUE count: {combined['high_risk_flag'].sum()}")
print(f"  FALSE count: {(~combined['high_risk_flag']).sum()}")

# Fuel alert flag (alert level is not NORMAL)
combined['fuel_alert'] = combined['alert_level'].isin(
    ['MONITOR', 'WATCH', 'WARNING', 'CRITICAL']
)

print(f"\nfuel_alert:")
print(f"  Formula: alert_level IN ('MONITOR', 'WATCH', 'WARNING', 'CRITICAL')")
print(f"  TRUE count: {combined['fuel_alert'].sum()}")
print(f"  FALSE count: {(~combined['fuel_alert']).sum()}")

# Dual concern flag (both high risk AND fuel alert)
combined['dual_concern'] = combined['high_risk_flag'] & combined['fuel_alert']

print(f"\ndual_concern:")
print(f"  Formula: high_risk_flag AND fuel_alert")
print(f"  TRUE count: {combined['dual_concern'].sum()}")
print(f"  FALSE count: {(~combined['dual_concern']).sum()}")

# ==============================================================================
# STEP 10: DISPLAY FINAL RESULTS
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 10: FINAL RESULTS")
print("=" * 80)

print(f"\n{'='*60}")
print("SUMMARY STATISTICS")
print(f"{'='*60}")
print(f"Total Drivers Analyzed: {len(combined)}")
print(f"High Risk Drivers (risk_factor >= 7.0): {combined['high_risk_flag'].sum()}")
print(f"Medium Risk Drivers (5.0 <= risk_factor < 7.0): {((combined['risk_factor'] >= 5.0) & (combined['risk_factor'] < 7.0)).sum()}")
print(f"Low Risk Drivers (risk_factor < 5.0): {(combined['risk_factor'] < 5.0).sum()}")
print(f"Drivers with Fuel Alerts: {combined['fuel_alert'].sum()}")
print(f"DUAL CONCERN Drivers: {combined['dual_concern'].sum()}")
print(f"Average Risk Factor: {combined['risk_factor'].mean():.2f}")
print(f"Maximum Risk Factor: {combined['risk_factor'].max():.2f}")

print(f"\n{'='*60}")
print("HIGH RISK DRIVERS (risk_factor >= 7.0)")
print(f"{'='*60}")

high_risk_drivers = combined[combined['high_risk_flag']].sort_values(
    'risk_factor', ascending=False
)

for _, row in high_risk_drivers.iterrows():
    dual_flag = "🚨 DUAL CONCERN" if row['dual_concern'] else ""
    fuel_flag = "⚠️ FUEL ALERT" if row['fuel_alert'] else "✓ Fuel OK"
    
    print(f"\nDriver {row['driverid']} | {row['model']} {dual_flag}")
    print(f"  Risk Factor: {row['risk_factor']}/10")
    print(f"  Components: base={row['base_event_risk']:.2f} + fuel={row['fuel_penalty']:.2f} + model={row['model_penalty']:.2f}")
    print(f"  Risky Events: {row['risky_event_count']}")
    print(f"  Current MPG: {row['avg_mpg']:.2f} | Historical: {row['hist_mean']:.2f}")
    print(f"  MPG Deviation: {row['pct_dev_personal']:.1f}%")
    print(f"  Alert Level: {row['alert_level']} | {fuel_flag}")
    print(f"  Location: {row['primary_city']}, {row['last_state']}")

print(f"\n{'='*60}")
print("DUAL CONCERN DRIVERS (High Risk + Fuel Anomaly)")
print(f"{'='*60}")

dual_concern_drivers = combined[combined['dual_concern']]

if len(dual_concern_drivers) > 0:
    for _, row in dual_concern_drivers.iterrows():
        print(f"\n🚨 Driver {row['driverid']} | {row['model']}")
        print(f"   Risk Factor: {row['risk_factor']}/10 (HIGH RISK)")
        print(f"   Risky Events: {row['risky_event_count']} (highest in fleet)")
        print(f"   Current MPG: {row['avg_mpg']:.2f}")
        print(f"   Historical MPG: {row['hist_mean']:.2f}")
        print(f"   MPG Deviation: {row['pct_dev_personal']:.1f}%")
        print(f"   Fuel Alert Level: {row['alert_level']}")
        print(f"   Location: {row['primary_city']}, {row['last_state']}")
        print(f"   Total Miles: {row['total_miles']:,.0f}")
else:
    print("\nNo drivers found with both high risk AND fuel anomalies.")

# ==============================================================================
# STEP 11: EXPORT TO CSV
# ==============================================================================
print("\n" + "=" * 80)
print("STEP 11: EXPORT TO comprehensive_risk_tableau.csv")
print("=" * 80)

# Select columns for export
export_columns = [
    'driverid', 'model', 'total_miles', 'total_gas', 'avg_mpg',
    'hist_mean', 'z_score_personal', 'pct_dev_personal', 'alert_level',
    'risky_event_count', 'base_event_risk', 'fuel_penalty', 'model_penalty',
    'risk_factor', 'risk_level', 'high_risk_flag', 'fuel_alert', 'dual_concern',
    'primary_city', 'last_state', 'last_latitude', 'last_longitude',
    'model_risky_events', 'city_risky_events'
]

# Export to CSV
output_path = '/mnt/user-data/outputs/comprehensive_risk_tableau.csv'
combined[export_columns].to_csv(output_path, index=False)

print(f"\nExported: {output_path}")
print(f"  - Rows: {len(combined)}")
print(f"  - Columns: {len(export_columns)}")

print(f"\nKey columns in output file:")
key_columns = {
    'driverid': 'Driver identifier',
    'risk_factor': 'Final risk score (1-10 scale)',
    'risk_level': 'Risk classification (LOW/MEDIUM/HIGH RISK)',
    'high_risk_flag': 'TRUE if risk_factor >= 7.0',
    'fuel_alert': 'TRUE if fuel efficiency is anomalous',
    'dual_concern': 'TRUE if both high_risk AND fuel_alert',
    'risky_event_count': 'Number of dangerous driving events',
    'base_event_risk': 'Risk points from events (1-8)',
    'fuel_penalty': 'Risk points from fuel anomaly (0-2)',
    'model_penalty': 'Risk points from truck model (0-1)',
    'z_score_personal': 'How many std devs from historical MPG',
    'pct_dev_personal': 'Percentage deviation from historical MPG',
    'alert_level': 'Fuel alert classification (NORMAL/MONITOR/WATCH/WARNING/CRITICAL)'
}

for col, desc in key_columns.items():
    print(f"  - {col}: {desc}")

print("\n" + "=" * 80)
print("SCRIPT 2 COMPLETE: comprehensive_risk_tableau.csv generated successfully")
print("=" * 80)

# ==============================================================================
# STEP 12: FORMULA REFERENCE CARD
# ==============================================================================
print("\n" + "=" * 80)
print("FORMULA REFERENCE CARD")
print("=" * 80)

print("""
┌─────────────────────────────────────────────────────────────────────────────┐
│ FUEL EFFICIENCY CALCULATIONS                                                │
├─────────────────────────────────────────────────────────────────────────────┤
│ avg_mpg          = total_miles / total_gas                                  │
│ hist_mean        = AVERAGE(monthly_mpg for 54 months)                       │
│ hist_std         = STDEV(monthly_mpg for 54 months)                         │
│ z_score_personal = (avg_mpg - hist_mean) / hist_std                         │
│ pct_dev_personal = ((avg_mpg - hist_mean) / hist_mean) × 100                │
├─────────────────────────────────────────────────────────────────────────────┤
│ ALERT LEVEL THRESHOLDS                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ |z_score| >= 2.5  →  CRITICAL                                               │
│ |z_score| >= 2.0  →  WARNING                                                │
│ |z_score| >= 1.5  →  WATCH                                                  │
│ |z_score| >= 1.0  →  MONITOR                                                │
│ |z_score| <  1.0  →  NORMAL                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ RISK FACTOR CALCULATION                                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│ base_event_risk  = 1 + (risky_event_count / 14) × 7        [range: 1-8]     │
│ fuel_penalty     = MIN(|z_score_personal|, 2)              [range: 0-2]     │
│ model_penalty    = model_risky_events / 89                 [range: 0-1]     │
│ risk_factor      = base_event_risk + fuel_penalty + model_penalty           │
│ risk_factor      = CLIP(risk_factor, 1, 10)                [range: 1-10]    │
├─────────────────────────────────────────────────────────────────────────────┤
│ FLAG CALCULATIONS                                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│ fuel_alert       = alert_level != 'NORMAL'                                  │
│ high_risk_flag   = risk_factor >= 7.0                                       │
│ dual_concern     = high_risk_flag AND fuel_alert                            │
└─────────────────────────────────────────────────────────────────────────────┘
""")
