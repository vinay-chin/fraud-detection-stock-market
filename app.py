import streamlit as st
import plotly.express as px
import pandas as pd
import orders
import utils

st.set_page_config(layout="wide")

page_bg_img = '''
<style>
[data-testid="stApp"]{
background-image: url("https://images.unsplash.com/photo-1542281286-9e0a16bb7366");
background-size: cover;
}
</style>
'''

st.markdown(page_bg_img, unsafe_allow_html=True)

# Load data
df = utils.read_parquet('Orders.parquet')
results = orders.detect_unusual_activities(df)
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

