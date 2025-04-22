import json
import os
import pandas as pd
import numpy as np
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# ✅ Read from environment variable, not from file
creds_dict = json.loads(os.environ["GSPREAD_CREDENTIALS_JSON"])

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Sheets
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

background_sheet = client.open("BackgroundAnalysisStore").sheet1

kite = get_kite(api_key, access_token)

symbols = [
    "RELIANCE", "INFY", "TCS", "ICICIBANK", "HDFCBANK", "SBIN", "BHARTIARTL",
    "ITC", "KOTAKBANK", "LT", "AXISBANK", "MARUTI", "ASIANPAINT", "SUNPHARMA",
    "HINDUNILVR", "BAJFINANCE", "WIPRO", "TECHM", "ULTRACEMCO", "POWERGRID",
    "NTPC", "ONGC", "JSWSTEEL", "TATASTEEL", "COALINDIA", "GRASIM", "NESTLEIND",
    "CIPLA", "DRREDDY", "TITAN", "ADANIENT", "ADANIPORTS", "BPCL", "BRITANNIA",
    "EICHERMOT", "HEROMOTOCO", "HINDALCO", "DIVISLAB", "INDUSINDBK", "BAJAJFINSV",
    "SBILIFE", "SHREECEM", "TATAMOTORS", "HCLTECH", "BAJAJ_AUTO", "APOLLOHOSP",
    "M&M"
]

TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5},
    "1d": {"interval": "day", "days": 90},
}

data = []

for symbol in symbols:
    row = {"Symbol": symbol}
    try:
        for label, config in TIMEFRAMES.items():
            df = get_stock_data(kite, symbol, config["interval"], config["days"])
            if df.empty:
                continue
            result = calculate_scores(df)
            row[f"{label} TMV Score"] = round(result["TMV Score"], 2)
            row[f"{label} Trend Direction"] = result["Trend Direction"]
            row[f"{label} Reversal Probability"] = round(result["Reversal Probability"], 2)
        ltp = df["close"].iloc[-1]
        previous_close = df["close"].iloc[-2] if len(df) >= 2 else ltp
        pct_change = ((ltp - previous_close) / previous_close) * 100
        row["LTP"] = round(ltp, 2)
        row["% Change"] = f"{pct_change:.2f}%"
        data.append(row)
    except Exception as e:
        print(f"❌ Error processing {symbol}: {e}")

# Upload to Google Sheet
if data:
    df_final = pd.DataFrame(data)
    background_sheet.clear()
    background_sheet.update([df_final.columns.values.tolist()] + df_final.values.tolist())
    print("✅ Background analysis uploaded to sheet.")
else:
    print("⚠️ No data available to upload.")
