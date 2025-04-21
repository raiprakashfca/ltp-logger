import os
import json
import pandas as pd
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils.indicators import calculate_scores

# ========== Setup ==========

# Load secrets from base64 if using Render environment
if "GSPREAD_CREDENTIALS_JSON" in os.environ:
    creds_dict = json.loads(
        os.environ["GSPREAD_CREDENTIALS_JSON"]
    )
else:
    with open("zerodhatokensaver-1b53153ffd25.json") as f:
        creds_dict = json.load(f)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Read tokens
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[:3]

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Timeframes and target sheet
TIMEFRAMES = {
    "15m": {"interval": "15minute"},
    "1d": {"interval": "day"}
}
TARGET_SHEET = "BackgroundAnalysisStore"

# Define list of NIFTY 50 stocks
symbols = [
    "RELIANCE", "INFY", "TCS", "ICICIBANK", "HDFCBANK", "SBIN", "BHARTIARTL", "AXISBANK", "ITC", "KOTAKBANK",
    "LT", "HCLTECH", "MARUTI", "TITAN", "ASIANPAINT", "SUNPHARMA", "NESTLEIND", "ULTRACEMCO", "BAJFINANCE", "HINDUNILVR",
    "WIPRO", "BAJAJFINSV", "POWERGRID", "NTPC", "ONGC", "COALINDIA", "TATASTEEL", "JSWSTEEL", "TECHM", "CIPLA",
    "DRREDDY", "DIVISLAB", "GRASIM", "ADANIENT", "ADANIPORTS", "BPCL", "BRITANNIA", "EICHERMOT", "HINDALCO", "HEROMOTOCO",
    "BAJAJ-AUTO", "SHREECEM", "INDUSINDBK", "SBILIFE", "APOLLOHOSP", "ICICIPRULI", "HDFCLIFE", "UPL", "M&M", "TATAMOTORS"
]

# ========== Helper Functions ==========

def get_instrument_token(kite, symbol):
    instruments = kite.instruments()
    for ins in instruments:
        if ins['tradingsymbol'] == symbol and ins['exchange'] == 'NSE':
            return ins['instrument_token']
    raise Exception(f"Token not found for {symbol}")

def get_stock_data(kite, symbol, interval):
    today = datetime.now()

    # Define historical range
    if interval == "15minute":
        from_date = today - timedelta(days=7)
    elif interval == "day":
        from_date = today - timedelta(days=130)
    else:
        from_date = today - timedelta(days=30)

    try:
        data = kite.historical_data(
            instrument_token=get_instrument_token(kite, symbol),
            from_date=from_date,
            to_date=today,
            interval=interval,
            continuous=False
        )
        df = pd.DataFrame(data)
        df.rename(columns={"date": "date"}, inplace=True)
        return df
    except Exception as e:
        print(f"⚠️ Error fetching data for {symbol} ({interval}): {e}")
        return pd.DataFrame()

# ========== Analysis ==========

rows = []
for symbol in symbols:
    row = {"Symbol": symbol}
    try:
        for tf_label, tf_config in TIMEFRAMES.items():
            df = get_stock_data(kite, symbol, tf_config["interval"])
            if df.empty:
                continue
            result = calculate_scores(df)
            row[f"{tf_label} TMV Score"] = round(result["TMV Score"], 2)
            row[f"{tf_label} Trend Direction"] = result["Trend Direction"]
            row[f"{tf_label} Reversal Probability"] = round(result["Reversal Probability"], 2)
        # Get last close price and LTP from daily data
        if not df.empty:
            row["LTP"] = df.iloc[-1]["close"]
            row["% Change"] = round(((df.iloc[-1]["close"] - df.iloc[-2]["close"]) / df.iloc[-2]["close"]) * 100, 2)
    except Exception as e:
        print(f"⚠️ Error processing {symbol}: {e}")
    rows.append(row)

# ========== Push to Google Sheets ==========

try:
    sheet = client.open(TARGET_SHEET).sheet1
    sheet.clear()
    headers = list(rows[0].keys())
    sheet.append_row(headers)
    for row in rows:
        sheet.append_row([row.get(col, "") for col in headers])
    print("✅ Sheet updated successfully.")
except Exception as e:
    print(f"❌ Failed to update Google Sheet: {e}")
