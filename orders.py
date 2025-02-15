import pandas as pd
from datetime import datetime, timedelta
import utils

def detect_unusual_activities(orders_df):
    orders_df = rename_columns(orders_df)

    suspicious_locations = detect_location_anomalies(orders_df)
    rapid_suspicious = detect_rapid_buysell(orders_df)

    results = {
        "sus_location_activity": len(suspicious_locations),
        "rapid_buy_sell": len(rapid_suspicious),
        "dangerous_traders": rapid_suspicious.to_dict(orient='records'),
        "dangerous_locations": suspicious_locations.to_dict(orient='records')
    }

    return results

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
        (location_activity['unique_clients'] < 4)
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
