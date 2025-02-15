import pandas as pd
import pyarrow.parquet as pq

# Read the Parquet file
table = pq.read_table("Trades.parquet")

# Convert to Pandas DataFrame
df = table.to_pandas()

# Filter out rows where both BUY_CLIENT_ID and SELL_CLIENT_ID are 0
df_filtered = df[(df["BUY_CLIENT_ID"] != 0) | (df["SELL_CLIENT_ID"] != 0)]

# Save the cleaned DataFrame back to Parquet
df_filtered.to_parquet("Trades_Cleaned.parquet", engine="pyarrow", index=False)

print("Filtered Parquet file saved as 'Trades_Cleaned.parquet'")
