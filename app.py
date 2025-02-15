import streamlit as st
import plotly.express as px
import pandas as pd
import orders
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

orders_file = st.file_uploader("Choose Orders.csv file")
trades_file = st.file_uploader("Choose Trades.csv file")

if orders_file is None or trades_file is None:
    st.info("Please upload both Orders.csv and Trades.csv files")
    st.stop()

orders_df = cleaning_data_from_orders(orders_file)
trades_df = cleaning_data_from_trading(trades_file)

st.write("Orders Table:")
st.write(orders_df.head(10))

st.write("Trades Table:")
st.write(trades_df.head(10))


results = orders.detect_unusual_activities(orders_df)
st.title("Fraud Detection Dashboard")
# Create three columns
col1, col2, col3 = st.columns(3)

# Display unusual activities in the first column
with col1:
    st.subheader("Unusual Activities")
    st.metric("Suspicious Location Activity", results['sus_location_activity'])
    st.metric("Rapid Buy-Sell (No Profit)", results['rapid_buy_sell'])

# Show dangerous traders in the second column
with col2:
    st.subheader("Dangerous Traders")
    df = pd.DataFrame(results['dangerous_traders'])
    fig = px.pie(df.head(20), names='TRADER_ID', values='total_orders')
    st.plotly_chart(fig, use_container_width=True)

    fig = px.scatter(df, x='TRADER_ID', y='total_orders', color='unique_clients')
    st.plotly_chart(fig, use_container_width=True)

# Show dangerous locations in the third column
with col3:
    st.subheader("Dangerous Locations")
    df = pd.DataFrame(results['dangerous_locations'])
    fig = px.scatter(df, x='LOCATION_ID', y='total_orders', color='unique_clients')
    st.plotly_chart(fig, use_container_width=True)
    st.write(df)

