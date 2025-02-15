import streamlit as st
import plotly.express as px
import pandas as pd
import orders
import Trade
import utils

st.set_page_config(layout="wide")

page_bg_img = '''
<style>
.stAppHeader {
background: rgb(14 17 23 / 0%);
}
[data-testid="stApp"]{
background-image: url("https://res.cloudinary.com/djqp7rqfd/image/upload/f_auto,q_auto/qj3pdzrhdhfajhm05phf");
background-size: cover;
}
</style>
'''

st.markdown(page_bg_img, unsafe_allow_html=True)

@st.cache_data
def cleaning_data_from_orders(file):
    df = pd.read_csv(file)
    df = utils.clean_data_orders(df)
    df = df.reset_index(drop=True)
    return df

@st.cache_data
def cleaning_data_from_trading(file):
    df = pd.read_csv(file)
    df = utils.clean_trades_data(df)
    df = df.reset_index(drop=True)
    return df

orders_file = st.sidebar.file_uploader("Choose Orders.csv file", type=["csv"])
trades_file = st.sidebar.file_uploader("Choose Trades.csv file", type=["csv"])

if orders_file is None or trades_file is None:
    st.info("Please upload both Orders.csv and Trades.csv files")
    st.stop()

orders_df = cleaning_data_from_orders(orders_file)
trades_df = cleaning_data_from_trading(trades_file)

st.write("Orders Table:")
st.write(orders_df.head(10))

st.write("Trades Table:")
st.write(trades_df.head(10))

st.title("Fraud Detection Dashboard")
# Create three columns
col1, col2, col3 = st.columns(3)

order_results = orders.detect_unusual_activities(orders_df)
trade_results = Trade.detect_anomalies(trades_df)
# Display unusual activities in the first column
with col1:
    st.subheader("Orders Unusual Activities")
    st.metric("Suspicious Location Activity", order_results['sus_location_activity'])
    st.metric("Rapid Buy-Sell (No Profit)", order_results['rapid_buy_sell'])

# Show dangerous traders in the second column
with col2:
    st.subheader("Orders Dangerous Traders")
    df = pd.DataFrame(order_results['dangerous_traders'])
    fig = px.pie(df.head(25), names='TRADER_ID', values='total_orders', title="Top 25 Traders by Total Orders")
    st.plotly_chart(fig, use_container_width=True)

    fig = px.scatter(df, x='TRADER_ID', y='total_orders', color='unique_clients', title="Traders by Total Orders and Unique Clients")
    st.plotly_chart(fig, use_container_width=True)

# Show dangerous locations in the third column
with col3:
    st.subheader("Orders Dangerous Locations")
    df = pd.DataFrame(order_results['dangerous_locations'])
    fig = px.scatter(df, x='LOCATION_ID', y='total_orders', color='unique_clients', title="Locations by Total Orders and Unique Clients")
    st.plotly_chart(fig, use_container_width=True)

# Create three columns
col1, col2, col3 = st.columns(3)

# Display unusual activities in the first column
with col1:
    st.subheader("Trades Unusual Activities")
    st.metric("Circular Trading", trade_results['circular_trading_count'])
    st.metric("Spoofing", trade_results['spoofing_count'])
    st.metric("Front Running", trade_results['front_running_count'])
    st.metric("Pump and Dump", trade_results['pump_and_dump_count'])
    st.metric("High-Frequency Trading", trade_results['high_frequency_trades_count'])

# Show dangerous traders in the second column
with col2:
    st.subheader("Trades Spoofing Records")
    df = pd.DataFrame(trade_results['spoofing_records'])
    fig = px.scatter(df, x='BUY_ORDER_ID', y='BUY_CLIENT_ID', color='TRADE_QUANTITY', title="Spoofing Records")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Trades Circular Trading Records")
    df = pd.DataFrame(trade_results['circular_trading_records'])
    fig = px.scatter(df, x='TRADE_TIME', y='TRADE_RATE', color='TRADE_QUANTITY', title="Circular Trading Records Over Time")
    st.plotly_chart(fig, use_container_width=True)

# Show dangerous locations in the third column
with col3:
    st.subheader("Trades Pump and Dump Records")
    df = pd.DataFrame(trade_results['pump_and_dump_records'])
    fig = px.scatter(df, x='TRADE_TIME', y='TRADE_RATE', color='TRADE_QUANTITY', title="Pump and Dump Records Over Time")
    st.plotly_chart(fig, use_container_width=True)

