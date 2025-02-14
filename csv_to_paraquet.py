import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def csv_to_parquet(csv_file):
    df = pd.read_csv(csv_file)
    table = pa.Table.from_pandas(df)
    parquet_file = os.path.splitext(csv_file)[0] + '.parquet'
    pq.write_table(table, parquet_file)

def read_paraquet(parquet_file):
    table = pq.read_table(parquet_file)
    print(table.to_pandas())

def clean_data(df):
    print(df.info())

# read_paraquet('Orders.parquet')
csv_to_parquet('Trades.csv')