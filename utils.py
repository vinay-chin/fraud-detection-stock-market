import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import random

def rename_columns(df):
    cols = df.columns
    new_cols = {}
    for col in cols:
        new_cols[col] = col.upper().strip().replace(' ', '_')
    df.rename(columns=new_cols, inplace=True)
    return df
def csv_to_parquet(csv_file):
    df = pd.read_csv(csv_file)
    table = pa.Table.from_pandas(df)
    parquet_file = os.path.splitext(csv_file)[0] + '.parquet'
    pq.write_table(table, parquet_file)

def read_parquet(parquet_file):
    table = pq.read_table(parquet_file)
    return table.to_pandas()

def fix_time(time_str):
    try:
        parts = time_str.split(':')
        if len(parts) == 2:
            return f"00:{parts[0]}:{parts[1]}"
        elif len(parts) == 3:
            hours = int(parts[0])
            return f"{hours % 24}:{parts[1]}:{parts[2]}"
    except Exception as e:
        print(f"Error fixing time: {e}")
    return None

def random_quantity(quantity, available_quantity):
    try:
        return random.randint(1, int(available_quantity))
    except Exception as e:
        print(f"Error generating random quantity: {e}")
    return quantity

def clean_data_orders(df):
    print("== Data Cleaning: Orders ==")
    print("Original Data:")
    print(df.info())
    
    df = df[df['QUANTITY'] > 0]
    print("Removed rows with non-positive QUANTITY.")
    
    df = df[df['QUANTITY'] <= df['AVAILABLE_QUANTITY']]
    print("Ensured QUANTITY does not exceed AVAILABLE_QUANTITY.")
    
    df = df[df['AVAILABLE_QUANTITY'] >= 0]
    print("Removed rows with negative AVAILABLE_QUANTITY.")
    
    df['QUANTITY'] = df.apply(lambda x: random_quantity(x['QUANTITY'], x['AVAILABLE_QUANTITY']), axis=1)
    
    df['order_time'] = df['order_time'].apply(fix_time)
    df = df.dropna(subset=['order_time'])
    print("Fixed and removed invalid order_time entries.")
    
    df = df.drop_duplicates(subset=['ORDER_SEQUENCE'])
    print("Removed duplicate ORDER_SEQUENCE entries.")
    
    df = df[df['order_id'].astype(str).str.len() <= 20]
    print("Ensured order_id length is within limits.")
    
    critical_columns = ['ORDER_SEQUENCE', 'order_id', 'order_time', 'order_date', 'scrip_code', 
                        'member_code', 'client_id', 'buy_OR_sell', 'RATE', 'QUANTITY', 
                        'AVAILABLE_QUANTITY']
    df = df.dropna(subset=critical_columns)
    print("Removed rows with missing data in critical columns.")
    
    print("\nCleaned Data:")
    print(df.head())
    print(f"\nTotal rows after cleaning: {len(df)}")
    return df

def clean_trades_data(df):
    print("== Data Cleaning: Trades ==")
    df = rename_columns(df)
    print(df.head())
    
    df = df[df['TRADE_QUANTITY'] > 0]
    print("Removed rows with non-positive TRADE_QUANTITY.")
    
    df = df[df['TRADE_RATE'] > 0]
    print("Removed rows with non-positive TRADE_RATE.")
    
    df['TRADE_TIME'] = df['TRADE_TIME'].apply(fix_time)
    df = df.dropna(subset=['TRADE_TIME'])
    print("Fixed and removed invalid TRADE_TIME entries.")
    
    df['TRADE_VALUE_CALC'] = df['TRADE_QUANTITY'] * df['TRADE_RATE']
    df = df[df['TRADE_VALUE'] == df['TRADE_VALUE_CALC']].drop(columns=['TRADE_VALUE_CALC'])
    print("Validated TRADE_VALUE consistency.")
    
    df = df.drop_duplicates(subset=['TRADE_SEQUENCE'])
    print("Removed duplicate TRADE_SEQUENCE entries.")
    
    critical_columns = ['TRADE_SEQUENCE', 'TRADE_NUMBER', 'TRADE_TIME', 'TRADE_DATE', 'SCRIP_CODE', 
                        'BUY_MEMBER_CODE', 'SELL_MEMBER_CODE', 'BUY_CLIENT_ID', 'SELL_CLIENT_ID', 
                        'BUY_ORDER_ID', 'SELL_ORDER_ID', 'BUY_TRADER_ID', 'SELL_TRADER_ID', 
                        'TRADE_QUANTITY', 'TRADE_RATE', 'TRADE_VALUE', 'BUY_LOCATION_ID', 
                        'SELL_LOCATION_ID', 'BUY_TIMESTAMP', 'SELL_TIMESTAMP']
    df = df.dropna(subset=critical_columns)
    print("Removed rows with missing data in critical columns.")
    
    print("\nCleaned Data:")
    print(df.head())
    print(f"\nTotal rows after cleaning: {len(df)}")
    return df

