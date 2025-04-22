import os
import json
import pandas as pd
from kiteconnect import KiteConnect
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# Decode GSpread credentials from base64 env var
creds_dict = json.loads(os.environ["GSPREAD_CREDENTIALS_JSON"])
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Read tokens from Google Sheet
tokens = client.open("ZerodhaTokenStore").sheet1.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

# Initialize Kite Connect
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Stock universe (skip HDFC as it's delisted)
symbols = [
    "RELIANCE", "TCS", "INFY", "ICICIBANK", "HDFCBANK", "KOTAKBANK",
    "LT", "SBIN", "AXISBANK", "BHARTIARTL", "ITC", "BAJFINANCE", "MARUTI",
    "SUNPHARMA", "ULTRACEMCO", "ASIANPAINT", "HCLTECH", "TITAN", "NESTLEIND",
    "WIPRO", "NTPC", "COALINDIA", "JSWSTEEL", "POWERGRID", "ONGC", "HINDUNILVR",
    "TECHM", "TATASTEEL", "DIVISLAB", "BAJAJ-AUTO", "EICHERMOT", "DRREDDY", "BRITANNIA",
    "HINDALCO", "BPCL", "ADANIENT", "GRASIM", "UPL", "CIPLA", "SBILIFE", "BAJAJFINSV",
    "HEROMOTOCO", "APOLLOHOSP", "ICICIPRULI", "INDUSINDBK", "SHREECEM", "TATACONSUM",
    "TATAMOTORS", "ADANIPORTS"
]

# Timeframes
TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5},
    "1d": {"interval": "day", "days": 90}
}

# Prepare final output
final_output = []

for symbol in symbols:
    print(f"Analyzing: {symbol}")
    row = {"Symbol": symbol}
    skip = False
    for label, config in TIMEFRAMES.items():
        try:
            df = get_stock_data(kite, symbol, config["interval"], config["days"])
            if df.empty:
                print(f"❌ No data for {symbol} at {label}")
                skip = True
                break
            scores = calculate_scores(df)
            row[f"{label} TMV Score"] = round(scores.get("TMV Score", 0), 2)
            row[f"{label} Trend Direction"] = scores.get("Trend Direction", "")
            row[f"{label} Reversal Probability"] = round(scores.get("Reversal Probability", 0), 2)
        except Exception as e:
            print(f"⚠️ Error for {symbol} at {label}: {e}")
            skip = True
            break
    if not skip:
        final_output.append(row)

# Convert to DataFrame
df = pd.DataFrame(final_output)

# Try to enrich with live LTP from LiveLTPStore (if available)
try:
    ltp_sheet = client.open("LiveLTPStore").sheet1
    ltp_data = pd.DataFrame(ltp_sheet.get_all_records())
    df = pd.merge(df, ltp_data, on="Symbol", how="left")
    df["% Change"] = df["% Change"].round(2)
except Exception as e:
    print(f"⚠️ Could not enrich with LTP data: {e}")

# Push to BackgroundAnalysisStore
try:
    bg_sheet = client.open("BackgroundAnalysisStore").sheet1
    bg_sheet.clear()
    bg_sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print("✅ Data pushed to BackgroundAnalysisStore")
except Exception as e:
    print(f"❌ Failed to update BackgroundAnalysisStore: {e}")
