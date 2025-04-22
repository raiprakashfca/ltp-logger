
import os
import json
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# Decode Base64 credentials if needed
creds_json = os.environ.get("GSPREAD_CREDENTIALS_JSON")
if creds_json.strip().startswith('{'):
    creds_dict = json.loads(creds_json)
else:
    import base64
    decoded = base64.b64decode(creds_json).decode("utf-8")
    creds_dict = json.loads(decoded)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Read token and initialize Kite
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

kite = get_kite(api_key, access_token)

# Define stock list (NIFTY 50)
nifty_50 = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS", "LT", "ITC", "KOTAKBANK", "SBIN", "BHARTIARTL",
    "BAJFINANCE", "HCLTECH", "ASIANPAINT", "MARUTI", "SUNPHARMA", "AXISBANK", "ULTRACEMCO", "WIPRO",
    "TITAN", "NESTLEIND", "TECHM", "HINDUNILVR", "POWERGRID", "HDFCLIFE", "ADANIENT", "NTPC", "BAJAJFINSV",
    "ONGC", "JSWSTEEL", "M&M", "DIVISLAB", "COALINDIA", "SBILIFE", "GRASIM", "BAJAJ-AUTO", "CIPLA", "DRREDDY",
    "BPCL", "EICHERMOT", "INDUSINDBK", "HEROMOTOCO", "HINDALCO", "TATASTEEL", "UPL", "BRITANNIA", "TATACONSUM",
    "SHREECEM", "APOLLOHOSP"
]

# Timeframes for analysis
TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5},
    "1d": {"interval": "day", "days": 90}
}

results = []

for symbol in nifty_50:
    print(f"📊 Processing {symbol}")
    row = {"Symbol": symbol}
    try:
        ltp = kite.ltp(f"NSE:{symbol}")[f"NSE:{symbol}"]["last_price"]
        row["LTP"] = round(ltp, 2)
    except:
        row["LTP"] = None

    for label, config in TIMEFRAMES.items():
        try:
            df = get_stock_data(kite, symbol, config["interval"], config["days"])
            if df.empty:
                raise Exception("No data")
            scores = calculate_scores(df)

            row[f"{label} TMV Score"] = round(scores["TMV Score"], 2)
            row[f"{label} Trend Direction"] = scores["Trend Direction"]
            row[f"{label} Reversal Probability"] = round(scores["Reversal Probability"], 2)
            if label == "1d":
                prev_close = df["close"].iloc[-2]
                row["% Change"] = f"{((ltp - prev_close)/prev_close)*100:.2f}%" if ltp else None
        except Exception as e:
            print(f"⚠️ Failed {symbol} - {label}: {e}")
            row[f"{label} TMV Score"] = 0
            row[f"{label} Trend Direction"] = "NA"
            row[f"{label} Reversal Probability"] = 0

    results.append(row)

# Save to Google Sheet
df = pd.DataFrame(results)
try:
    sheet = client.open("BackgroundAnalysisStore").sheet1
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print("✅ Data updated in BackgroundAnalysisStore")
except Exception as e:
    print(f"❌ Sheet update failed: {e}")
