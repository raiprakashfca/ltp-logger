
import os
import json
import base64
import pandas as pd
import gspread
from kiteconnect import KiteConnect
from oauth2client.service_account import ServiceAccountCredentials
from utils.indicators import calculate_scores
from utils.zerodha import get_kite, get_stock_data

# Load Google Sheet credentials from base64 environment variable
creds_dict = json.loads(base64.b64decode(os.environ["GSPREAD_CREDENTIALS_JSON"]).decode("utf-8"))
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Load token from sheet
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

# Initialize Kite
kite = get_kite(api_key, access_token)

# Static list of NIFTY 50 stocks (update as needed)
symbols = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "SBIN", "LT",
    "ITC", "BHARTIARTL", "ASIANPAINT", "HCLTECH", "WIPRO", "SUNPHARMA", "ULTRACEMCO",
    "BAJFINANCE", "AXISBANK", "TECHM", "MARUTI", "NTPC", "POWERGRID", "NESTLEIND",
    "HINDUNILVR", "ONGC", "TITAN", "JSWSTEEL", "GRASIM", "TATASTEEL", "COALINDIA",
    "DRREDDY", "CIPLA", "BRITANNIA", "ADANIENT", "ADANIPORTS", "EICHERMOT", "HINDALCO",
    "SBILIFE", "DIVISLAB", "HEROMOTOCO", "APOLLOHOSP", "BAJAJFINSV", "HDFCLIFE",
    "BAJAJ-AUTO", "INDUSINDBK", "BPCL", "UPL", "SHREECEM", "M&M"
]

output = []
for symbol in symbols:
    try:
        df_daily = get_stock_data(kite, symbol, interval="day", days=90)
        df_15m = get_stock_data(kite, symbol, interval="15minute", days=5)
        if df_daily.empty or df_15m.empty:
            continue
        latest_close = df_daily.iloc[-2]["close"]
        latest_ltp = df_daily.iloc[-1]["close"]
        pct_change = round((latest_ltp - latest_close) / latest_close * 100, 2)
        result_15m = calculate_scores(df_15m)
        result_daily = calculate_scores(df_daily)
        output.append({
            "Symbol": symbol,
            "LTP": latest_ltp,
            "% Change": f"{pct_change}%",

            "15m TMV Score": round(result_15m.get("TMV Score", 0), 2),
            "15m Trend Direction": result_15m.get("Trend Direction", "-"),
            "15m Reversal Probability": round(result_15m.get("Reversal Probability", 0), 2),

            "1d TMV Score": round(result_daily.get("TMV Score", 0), 2),
            "1d Trend Direction": result_daily.get("Trend Direction", "-"),
            "1d Reversal Probability": round(result_daily.get("Reversal Probability", 0), 2),
        })
    except Exception as e:
        print(f"❌ {symbol} failed: {e}")

# Write to Google Sheet
if output:
    df = pd.DataFrame(output)
    try:
        sheet = client.open("BackgroundAnalysisStore").sheet1
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print("✅ Sheet updated successfully.")
    except Exception as e:
        print("❌ Sheet update failed:", e)
else:
    print("⚠️ No data to update.")
