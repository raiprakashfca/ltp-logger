import os
import json
import base64
import pandas as pd
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# Authenticate with Google Sheets using BASE64 environment variable
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
encoded_credentials = os.environ.get("GSPREAD_CREDENTIALS_JSON")
decoded_json = json.loads(base64.b64decode(encoded_credentials).decode())
creds = ServiceAccountCredentials.from_json_keyfile_dict(decoded_json, scope)
client = gspread.authorize(creds)

# Load Zerodha token
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

# Set up Kite
kite = get_kite(api_key, access_token)

# Output Google Sheet
output_sheet = client.open("BackgroundAnalysisStore").sheet1

# Stocks to track
symbols = [
    "RELIANCE", "INFY", "TCS", "ICICIBANK", "HDFCBANK", "SBIN", "BHARTIARTL",
    "ITC", "LT", "AXISBANK", "KOTAKBANK", "ASIANPAINT", "MARUTI", "ULTRACEMCO",
    "SUNPHARMA", "WIPRO", "TITAN", "HCLTECH", "TECHM", "BAJFINANCE", "HINDUNILVR",
    "NESTLEIND", "BAJAJFINSV", "POWERGRID", "NTPC", "ONGC", "JSWSTEEL", "GRASIM",
    "HINDALCO", "CIPLA", "DRREDDY", "ADANIENT", "ADANIPORTS", "COALINDIA", "TATASTEEL",
    "UPL", "BPCL", "BRITANNIA", "DIVISLAB", "SBILIFE", "HEROMOTOCO", "EICHERMOT", "BAJAJ-AUTO"
]

TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5},
    "1d": {"interval": "day", "days": 90}
}

results = []

for symbol in symbols:
    row = {"Symbol": symbol}
    try:
        for tf, config in TIMEFRAMES.items():
            df = get_stock_data(kite, symbol, config["interval"], config["days"])
            if df.empty:
                raise ValueError("No data returned for", symbol)
            scores = calculate_scores(df)
            row[f"{tf} TMV Score"] = round(scores.get("Total Score", 0), 2)
            row[f"{tf} Trend Direction"] = scores.get("Trend Direction", "-")
            row[f"{tf} Reversal Probability"] = round(scores.get("Reversal Probability", 0), 2)
        row["LTP"] = df["close"].iloc[-1]
        row["% Change"] = round((df["close"].iloc[-1] - df["close"].iloc[-2]) / df["close"].iloc[-2] * 100, 2)
        results.append(row)
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        continue

# Write to Google Sheet
if results:
    df = pd.DataFrame(results)
    output_sheet.clear()
    output_sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"✅ Analysis complete. {len(df)} stocks updated at {datetime.now()}")
else:
    print("⚠️ No data processed.")
