
import os
import json
import pandas as pd
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# Load Google Sheet credentials from BASE64
creds_dict = json.loads(os.environ["GSPREAD_CREDENTIALS_JSON"])
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Try to fetch tokens
try:
    token_sheet = client.open("ZerodhaTokenStore").sheet1
    tokens = token_sheet.get_all_values()[0]
    api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]
except Exception as e:
    print(f"❌ Could not fetch tokens: {e}")
    exit(1)

# Try to initialize kite client
try:
    kite = get_kite(api_key, access_token)
except Exception as e:
    print(f"❌ Could not initialize Kite: {e}")
    exit(1)

# Static stock list for now
symbols = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "LT", "SBIN", "AXISBANK", "KOTAKBANK", "ITC",
    "HINDUNILVR", "BAJFINANCE", "BHARTIARTL", "ASIANPAINT", "HCLTECH", "MARUTI", "SUNPHARMA",
    "TITAN", "ULTRACEMCO", "WIPRO", "NTPC", "POWERGRID", "INDUSINDBK", "TECHM", "NESTLEIND",
    "ONGC", "TATAMOTORS", "JSWSTEEL", "COALINDIA", "ADANIENT", "BPCL", "GRASIM", "DIVISLAB",
    "SBILIFE", "DRREDDY", "TATASTEEL", "BAJAJFINSV", "BRITANNIA", "HDFCLIFE", "HINDALCO",
    "CIPLA", "BAJAJ_AUTO", "HEROMOTOCO", "EICHERMOT", "APOLLOHOSP", "ADANIPORTS", "UPL",
    "SHREECEM", "M&M"
]

results = []

for symbol in symbols:
    try:
        row = {"Symbol": symbol}

        for tf_label, tf_info in {"15m": ("15minute", 5), "1d": ("day", 90)}.items():
            interval, days = tf_info
            df = get_stock_data(kite, symbol, interval, days)
            if df.empty:
                continue

            scores = calculate_scores(df)

            row[f"{tf_label} TMV Score"] = round(scores.get("TMV Score", 0), 2)
            row[f"{tf_label} Trend Direction"] = scores.get("Trend Direction", "Neutral")
            row[f"{tf_label} Reversal Probability"] = round(scores.get("Reversal Probability", 0), 2)

            if tf_label == "1d":
                row["LTP"] = df["close"].iloc[-1]
                prev_close = df["close"].iloc[-2] if len(df) >= 2 else df["close"].iloc[-1]
                row["% Change"] = round((df["close"].iloc[-1] - prev_close) / prev_close * 100, 2)

        results.append(row)

    except Exception as e:
        print(f"⚠️ Failed for {symbol}: {e}")

# Write to Google Sheet
if results:
    try:
        sheet = client.open("BackgroundAnalysisStore").sheet1
        df = pd.DataFrame(results)
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"✅ Uploaded {len(results)} records to BackgroundAnalysisStore")
    except Exception as e:
        print(f"❌ Could not update sheet: {e}")
else:
    print("⚠️ No results generated.")
