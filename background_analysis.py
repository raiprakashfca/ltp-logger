
import os
import json
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# Load Google Sheet credentials from environment
creds_dict = json.loads(os.environ.get("GSPREAD_CREDENTIALS_JSON", "{}"))

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Get token details
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[:3]

# Get Kite object
kite = get_kite(api_key, access_token)

# Timeframes
TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5},
    "1d": {"interval": "day", "days": 90},
}

symbols = [
    "RELIANCE", "INFY", "TCS", "ICICIBANK", "HDFCBANK", "SBIN", "BHARTIARTL", "LT",
    "KOTAKBANK", "ITC", "ASIANPAINT", "HCLTECH", "WIPRO", "SUNPHARMA", "NESTLEIND",
    "AXISBANK", "MARUTI", "BAJFINANCE", "TECHM", "TITAN", "ULTRACEMCO", "POWERGRID",
    "COALINDIA", "NTPC", "ONGC", "GRASIM", "ADANIENT", "ADANIPORTS", "BPCL", "CIPLA",
    "DIVISLAB", "DRREDDY", "EICHERMOT", "HINDALCO", "HEROMOTOCO", "JSWSTEEL",
    "M&M", "BAJAJFINSV", "BAJAJ-AUTO", "BRITANNIA", "HINDUNILVR", "TATACONSUM",
    "TATASTEEL", "SBILIFE", "SHREECEM", "HDFCLIFE", "ICICIPRULI", "INDUSINDBK",
    "APOLLOHOSP", "UPL"
]

all_data = []

for symbol in symbols:
    row = {"Symbol": symbol}
    for label, config in TIMEFRAMES.items():
        try:
            df = get_stock_data(kite, symbol, config["interval"], config["days"])
            if not df.empty:
                scores = calculate_scores(df)
                for key, value in scores.items():
                    adjusted_key = "TMV Score" if key == "Total Score" else key
                    row[f"{label} {adjusted_key}"] = value
                row["LTP"] = df['close'].iloc[-1]
                if "1d close" not in row and "1d TMV Score" in row:
                    row["1d close"] = df['close'].iloc[-2] if len(df) > 1 else df['close'].iloc[-1]
        except Exception as e:
            print(f"Error with {symbol} {label}: {e}")
    if "LTP" in row and "1d close" in row:
        row["% Change"] = ((row["LTP"] - row["1d close"]) / row["1d close"]) * 100
    all_data.append(row)

df = pd.DataFrame(all_data)
df = df.drop(columns=["1d close"], errors="ignore")
df["% Change"] = df["% Change"].apply(lambda x: f"{x:.2f}%")

# Reorder columns
column_order = ["Symbol", "LTP", "% Change"] + [col for col in df.columns if col not in ["Symbol", "LTP", "% Change"]]
df = df[column_order]

# Upload to Google Sheet
try:
    sheet = client.open("BackgroundAnalysisStore").sheet1
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print("✅ Background data uploaded.")
except Exception as e:
    print(f"❌ Failed to upload: {e}")
