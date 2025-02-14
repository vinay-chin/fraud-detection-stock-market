import pandas as pd
from datetime import datetime, timedelta
import utils

# Load cleaned orders data (replace with your file path)
orders_df = utils.read_paraquet('orders.parquet')

def detect_circular_trading(df):
    # Group by scrip and check if buyers/sellers are the same group
    circular_trades = df.groupby(['SCRIP_CODE', 'ORDER_DATE']).apply(
        lambda x: x[x['BUY_CLIENT_ID'].isin(x['SELL_CLIENT_ID'])] & 
                  x[x['SELL_CLIENT_ID'].isin(x['BUY_CLIENT_ID'])]
    )
    return circular_trades

def rename_columns(df):
    cols = df.columns
    new_cols = {}
    for col in cols:
        new_cols[col] = col.upper().strip().replace(' ', '_')
    df.rename(columns=new_cols, inplace=True)
    return df

def detect_location_anomalies(df):

    location_activity = df.groupby(['LOCATION_ID', 'SCRIP_CODE', 'ORDER_DATE']).agg(
        total_orders=('ORDER_ID', 'count'),
        unique_clients=('CLIENT_ID', 'nunique')
    ).reset_index()
 
    suspicious_locations = location_activity[
        (location_activity['total_orders'] > 50) & 
        (location_activity['unique_clients'] < 3)
    ]
    return suspicious_locations

def detect_rapid_buysell(df):
    grouped = df.groupby(['TRADER_ID', 'SCRIP_CODE', 'ORDER_DATE']).agg(
        total_orders=('ORDER_ID', 'count'),
        unique_clients=('TRADER_ID', 'nunique')
    ).reset_index()
    rapid_suspicious = grouped[
        (grouped['total_orders'] > 10) & 
        (grouped['unique_clients'] < 3)
    ].sort_values(by='total_orders', ascending=False).reset_index(drop=True)
    return rapid_suspicious


orders_df = rename_columns(orders_df)
orders_df.info()

suspicious_locations = detect_location_anomalies(orders_df)
print(suspicious_locations)

rapid_suspicious = detect_rapid_buysell(orders_df)
print(rapid_suspicious)

results = {
    "Suspicious Location Activity": len(suspicious_locations),
    "Rapid Buy-Sell (No Profit)": len(rapid_suspicious),
}

print("=== Unusual Activities Detected ===")
for activity, count in results.items():
    print(f"{activity}: {count} orders")