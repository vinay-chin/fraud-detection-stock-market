import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

# Load datasets (assume CSV format)
orders = pd.read_csv('orders.csv')
trades = pd.read_csv('trades.csv')

# Data Preprocessing
def preprocess_data(df):
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    df['ORDER_TIME'] = pd.to_datetime(df['ORDER_TIME'])
    df['TRADE_TIME'] = pd.to_datetime(df['TRADE_TIME'])
    return df

orders = preprocess_data(orders)
trades = preprocess_data(trades)

# Rule-Based Filtering (High-Frequency & Round-Trip Trades)
def detect_suspicious_trades(trades):
    trade_counts = trades.groupby(['BUY_CLIENT_ID', 'SELL_CLIENT_ID', 'SCRIP_CODE']).size()
    high_freq_trades = trade_counts[trade_counts > 10].reset_index()
    return high_freq_trades

suspicious_trades = detect_suspicious_trades(trades)

# Anomaly Detection using Isolation Forest
features = ['TRADE_QUANTITY', 'TRADE_RATE']
scaler = StandardScaler()
X = scaler.fit_transform(trades[features])

iso_forest = IsolationForest(contamination=0.01, random_state=42)
predictions = iso_forest.fit_predict(X)
trades['Anomaly'] = predictions

# Clustering-based Detection using DBSCAN
dbscan = DBSCAN(eps=0.5, min_samples=5)
trades['Cluster'] = dbscan.fit_predict(X)

# Graph-Based Analysis
def build_trade_graph(trades):
    G = nx.DiGraph()
    for _, row in trades.iterrows():
        G.add_edge(row['BUY_CLIENT_ID'], row['SELL_CLIENT_ID'], scrip=row['SCRIP_CODE'])
    return G

trade_graph = build_trade_graph(trades)

# Detecting Cycles in Trade Network
cycles = list(nx.simple_cycles(trade_graph))
print(f"Detected {len(cycles)} circular trading patterns.")

# Visualization of the Trade Network
plt.figure(figsize=(10, 8))
nx.draw(trade_graph, with_labels=True, node_color='lightblue', edge_color='gray')
plt.title("Trade Network Graph")
plt.show()

# Flagging Suspicious Clients
suspicious_clients = set()
for cycle in cycles:
    suspicious_clients.update(cycle)

print(f"Suspicious Clients Involved in Circular Trading: {suspicious_clients}")