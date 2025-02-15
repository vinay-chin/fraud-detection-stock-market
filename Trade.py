import pandas as pd
import pyarrow.parquet as pq

def detect_anomalies(df):
    # Convert Trade Time to Datetime
    df["TRADE_TIME"] = pd.to_datetime(df["TRADE_TIME"], errors="coerce")

    # Remove Trades Where Both Client IDs are 0
    df = df[(df["BUY_CLIENT_ID"] != 0) | (df["SELL_CLIENT_ID"] != 0)]

    ### 1️⃣ Circular Trading (Wash Trades)
    df["TIME_DIFF"] = df.groupby("BUY_CLIENT_ID")["TRADE_TIME"].diff().dt.seconds
    wash_trades = df[
        (df["BUY_CLIENT_ID"] == df["SELL_CLIENT_ID"]) &
        (df["TRADE_RATE"].duplicated()) &
        (df["TRADE_QUANTITY"].duplicated()) &
        (df["TIME_DIFF"] < 60)  # Trades happening within 1 min
    ][["TRADE_NUMBER", "TRADE_TIME", "BUY_CLIENT_ID", "TRADE_RATE", "TRADE_QUANTITY"]]

    ### 2️⃣ Spoofing (Fake Orders)
    spoof_orders = df[~df["BUY_ORDER_ID"].isin(df["TRADE_NUMBER"])][["BUY_ORDER_ID", "BUY_CLIENT_ID", "TRADE_QUANTITY"]]

    ### 3️⃣ Front Running (Insider Trading)
    df["TIME_DIFF"] = df.groupby("BUY_MEMBER_CODE")["TRADE_TIME"].diff().dt.seconds
    front_running = df[(df["TIME_DIFF"] > 0) & (df["TIME_DIFF"] < 120)][["TRADE_NUMBER", "TRADE_TIME", "BUY_MEMBER_CODE", "TRADE_QUANTITY"]]

    ### 4️⃣ Pump and Dump (Price Manipulation)
    df["PRICE_DIFF"] = df["TRADE_RATE"].pct_change()
    df["VOLUME_DIFF"] = df["TRADE_QUANTITY"].pct_change()
    pump_and_dump = df[
        (df["PRICE_DIFF"] > 0.1) &  # Price jumps 10%+
        (df["VOLUME_DIFF"] > 0.5)   # Volume jumps 50%+
    ][["TRADE_NUMBER", "TRADE_TIME", "SCRIP_CODE", "TRADE_RATE", "TRADE_QUANTITY"]]

    ### 5️⃣ High-Frequency Trading Anomalies
    df["TIME_DIFF"] = df.groupby(["BUY_CLIENT_ID", "SELL_CLIENT_ID"])["TRADE_TIME"].diff().dt.microseconds
    high_freq_trades = df[df["TIME_DIFF"] < 100000][["TRADE_NUMBER", "TRADE_TIME", "BUY_CLIENT_ID", "SELL_CLIENT_ID"]]

    # Prepare Results Dictionary
    results = {
        "circular_trading_count": len(wash_trades),
        "spoofing_count": len(spoof_orders),
        "front_running_count": len(front_running),
        "pump_and_dump_count": len(pump_and_dump),
        "high_frequency_trades_count": len(high_freq_trades),
        "circular_trading_records": wash_trades.head(10).to_dict(orient="records"),
        "spoofing_records": spoof_orders.head(10).to_dict(orient="records"),
        "front_running_records": front_running.head(10).to_dict(orient="records"),
        "pump_and_dump_records": pump_and_dump.head(10).to_dict(orient="records"),
        "high_frequency_trades_records": high_freq_trades.head(10).to_dict(orient="records"),
    }

    # Print only summarized result
    return results
