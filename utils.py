import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import random

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
            if hours >= 24:
                return f"{hours % 24}:{parts[1]}:{parts[2]}"
            else:
                return time_str
        else:
            return None 
    except:
        return None 

def random_quantity(QUANTITY, AVAILABLE_QUANTITY):
    try:
        QUANTITY = random.randint(1, int(AVAILABLE_QUANTITY))
    except:
        return QUANTITY
    return QUANTITY
    


def clean_data_orders(df):
    df = pd.read_csv('orders.csv')

    print("Original Data:")
    print(df.head())

    df = df[df['QUANTITY'] > 0]

    df = df[df['QUANTITY'] <= df['AVAILABLE_QUANTITY']]

    df = df[df['AVAILABLE_QUANTITY'] >= 0]

    df['QUANTITY'] = zip(*df.apply(lambda x: random_quantity(x['QUANTITY'], x['AVAILABLE_QUANTITY']), axis=1))

    df['order_time'] = df['order_time'].apply(fix_time)

    df = df.dropna(subset=['order_time'])

    df = df.drop_duplicates(subset=['ORDER_SEQUENCE'])

    df = df[df['order_id'].astype(str).str.len() <= 20] 

    critical_columns = ['ORDER_SEQUENCE', 'order_id', 'order_time', 'order_date', 'scrip_code', 'member_code', 'client_id', 'buy_OR_sell', 'RATE', 'QUANTITY', 'AVAILABLE_QUANTITY']
    df = df.dropna(subset=critical_columns)

    df.to_csv('cleaned_orders.csv', index=False)

    print("\nCleaned Data:")
    print(df.head())
    print(f"\nTotal rows after cleaning: {len(df)}")

def clean_trades_data(file_path, output_file):
    # Load the data
    df = pd.read_csv(file_path)

    # Display the first few rows to understand the data
    print("Original Data:")
    print(df.head())

    # Data Cleaning Steps

    # 1. Remove rows where TRADE_QUANTITY is 0 or negative
    df = df[df['TRADE_QUANTITY'] > 0]

    # 2. Remove rows where TRADE_RATE is 0 or negative
    df = df[df['TRADE_RATE'] > 0]

    # 3. Fix invalid TRADE_TIME (e.g., "37:59.7" should be converted to "00:37:59.7")
    df['TRADE_TIME'] = df['TRADE_TIME'].apply(fix_time)

    # Remove rows with invalid TRADE_TIME
    df = df.dropna(subset=['TRADE_TIME'])

    # 4. Ensure TRADE_VALUE is consistent with TRADE_QUANTITY * TRADE_RATE
    df['TRADE_VALUE_CALC'] = df['TRADE_QUANTITY'] * df['TRADE_RATE']
    df = df[df['TRADE_VALUE'] == df['TRADE_VALUE_CALC']]
    df = df.drop(columns=['TRADE_VALUE_CALC'])

    # 5. Remove rows with duplicate TRADE_SEQUENCE
    df = df.drop_duplicates(subset=['TRADE_SEQUENCE'])

    # 6. Remove rows with missing or invalid data in critical columns
    critical_columns = ['TRADE_SEQUENCE', 'TRADE_NUMBER', 'TRADE_TIME', 'TRADE_DATE', 'SCRIP_CODE', 
                        'BUY_MEMBER_CODE', 'SELL_MEMBER_CODE', 'BUY_CLIENT_ID', 'SELL_CLIENT_ID', 
                        'BUY_ORDER_ID', 'SELL_ORDER_ID', 'BUY_TRADER_ID', 'SELL_TRADER_ID', 
                        'TRADE_QUANTITY', 'TRADE_RATE', 'TRADE_VALUE', 'BUY_LOCATION_ID', 
                        'SELL_LOCATION_ID', 'BUY_TIMESTAMP', 'SELL_TIMESTAMP']
    df = df.dropna(subset=critical_columns)

    # Save the cleaned data to a new file
    df.to_csv(output_file, index=False)

    # Display the cleaned data
    print("\nCleaned Data:")
    print(df.head())
    print(f"\nTotal rows after cleaning: {len(df)}")

# Example usage
# clean_trades_data('Trades.csv', 'cleaned_trades.csv')

# pd = read_parquet('Orders.parquet')
# pd.info()
# csv_to_parquet('Trades.csv')

# df = pd.read_csv('Orders.csv')
# clean_data_orders(df)




