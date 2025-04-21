
import os
import sys
import pandas as pd
import json
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# Ensure utils directory is on the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# Setup credentials
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(os.environ["GSPREAD_CREDENTIALS_JSON"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Fetch credentials from ZerodhaTokenStore sheet
token_sheet = client.open("ZerodhaTokenStore").sheet1
api_key, api_secret, access_token = token_sheet.get_all_values()[0][:3]

# Initialize Kite (try live, fallback to offline)
try:
    kite = get_kite(api_key, access_token)
    market_live = True
except Exception as e:
    print("⚠️ Live mode failed. Switching to offline mode:", e)
    kite = None
    market_live = False

# Define stock list and timeframes
symbols = [
    "RELIANCE", "INFY", "TCS", "ICICIBANK", "HDFCBANK", "SBIN", "BHARTIARTL",
    "LT", "KOTAKBANK", "ITC", "HCLTECH", "WIPRO", "AXISBANK", "ASIANPAINT",
    "BAJFINANCE", "HINDUNILVR", "MARUTI", "SUNPHARMA", "ULTRACEMCO", "TITAN"
]

TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5},
    "1d": {"interval": "day", "days": 90},
}

# Collect data
all_rows = []
for symbol in symbols:
    row = {"Symbol": symbol}
    for label, config in TIMEFRAMES.items():
        if market_live:
            df = get_stock_data(kite, symbol, config["interval"], config["days"])
        else:
            # fallback to dummy data
            from utils.samples import get_sample_data
            df = get_sample_data(symbol, config["interval"], config["days"])

        if not df.empty:
            result = calculate_scores(df)
            row[f"{label} TMV Score"] = result.get("Total Score", "")
            row[f"{label} Trend Direction"] = result.get("Trend Direction", "")
            row[f"{label} Reversal Probability"] = result.get("Reversal Probability", "")
    all_rows.append(row)

# Save to Google Sheet
sheet = client.open("BackgroundAnalysisStore").sheet1
sheet.clear()
headers = [
    "Symbol", "15m TMV Score", "15m Trend Direction", "15m Reversal Probability",
    "1d TMV Score", "1d Trend Direction", "1d Reversal Probability"
]
sheet.append_row(headers)
sheet.append_rows([ [row.get(h, "") for h in headers] for row in all_rows ])
print("✅ Sheet updated successfully")
