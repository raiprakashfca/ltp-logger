# 🛠 Trigger clean rebuild on Render (2025-04-22)

import pandas as pd
import numpy as np
import json
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(Path("gspread_credentials.json").read_text())
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Sheet where analysis will be logged
sheet = client.open("BackgroundAnalysisStore").sheet1

# Symbols to analyze (NIFTY 50 stocks - HDFC excluded)
symbols = [
    "RELIANCE", "INFY", "TCS", "ICICIBANK", "HDFCBANK", "SBIN", "BHARTIARTL", "LT", "ITC",
    "KOTAKBANK", "AXISBANK", "ASIANPAINT", "MARUTI", "SUNPHARMA", "ULTRACEMCO", "TITAN",
    "BAJFINANCE", "WIPRO", "TECHM", "POWERGRID", "TATASTEEL", "HINDUNILVR", "HCLTECH",
    "COALINDIA", "ADANIENT", "ADANIPORTS", "BPCL", "CIPLA", "EICHERMOT", "GRASIM", "HINDALCO",
    "JSWSTEEL", "NTPC", "ONGC", "UPL", "SBILIFE", "DRREDDY", "BRITANNIA", "DIVISLAB",
    "BAJAJ-AUTO", "TATAMOTORS", "HEROMOTOCO", "INDUSINDBK", "BAJAJFINSV", "NESTLEIND", "APOLLOHOSP"
]

# Timeframes for analysis
TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5},
    "1d": {"interval": "day", "days": 90}
}

# Initialize Kite
tokens = client.open("ZerodhaTokenStore").sheet1.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]
kite = get_kite(api_key, access_token)

results = []

for symbol in symbols:
    row = {"Symbol": symbol}
    for label, tf in TIMEFRAMES.items():
        try:
            df = get_stock_data(kite, symbol, tf["interval"], tf["days"])
            if not df.empty:
                scores = calculate_scores(df)
                row[f"{label} TMV Score"] = round(scores.get("TMV Score", 0), 2)
                row[f"{label} Trend Direction"] = scores.get("Trend Direction", "")
                row[f"{label} Reversal Probability"] = round(scores.get("Reversal Probability", 0), 2)
                row["LTP"] = df["close"].iloc[-1]
                row["% Change"] = round((df["close"].iloc[-1] - df["close"].iloc[-2]) / df["close"].iloc[-2] * 100, 2)
        except Exception as e:
            print(f"Failed {symbol} {label}: {e}")
    results.append(row)

# Convert to DataFrame and write to sheet
df_result = pd.DataFrame(results)
df_result.fillna("", inplace=True)

sheet.clear()
sheet.update([df_result.columns.tolist()] + df_result.values.tolist())
print("✅ Background Analysis successfully updated at", datetime.now())
