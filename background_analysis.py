import os
import json
import base64
import pandas as pd
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores
# ==================== DEBUG BLOCK START ====================
st.subheader("🔍 Debug Secrets Check")
st.write("Secrets keys detected:", list(st.secrets.keys()))

try:
    service_account_info = dict(st.secrets["gcp_service_account"])
    st.success("✅ gcp_service_account block found!")
    st.json(service_account_info)
except Exception as e:
    st.error(f"❌ Failed to read gcp_service_account: {e}")
# ==================== DEBUG BLOCK END ======================
# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Base64 decode credentials
b64_creds = os.environ.get("GSPREAD_CREDENTIALS_JSON", "")
if not b64_creds:
    raise ValueError("❌ GSPREAD_CREDENTIALS_JSON environment variable is missing or empty!")

try:
    creds_json = base64.b64decode(b64_creds).decode("utf-8")
    creds_dict = json.loads(creds_json)
except Exception as e:
    raise ValueError(f"❌ Failed to decode or load Google Sheet credentials: {e}")

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Symbols to analyze
symbols = [
    "RELIANCE", "TCS", "INFY", "ICICIBANK", "HDFCBANK", "SBIN", "LT", "AXISBANK",
    "ITC", "KOTAKBANK", "BAJFINANCE", "HINDUNILVR", "HCLTECH", "WIPRO",
    "BHARTIARTL", "ASIANPAINT", "MARUTI", "SUNPHARMA", "NESTLEIND", "ULTRACEMCO",
    "TECHM", "POWERGRID", "TITAN", "NTPC", "BAJAJFINSV", "JSWSTEEL", "TATAMOTORS",
    "COALINDIA", "ONGC", "INDUSINDBK", "DRREDDY", "CIPLA", "ADANIENT", "SBILIFE",
    "DIVISLAB", "BPCL", "HINDALCO", "GRASIM", "TATASTEEL", "BAJAJ_AUTO",
    "BRITANNIA", "EICHERMOT", "HEROMOTOCO", "SHREECEM", "APOLLOHOSP", "UPL"
]

# Try to load previous LTPs
try:
    sheet = client.open("BackgroundAnalysisStore").sheet1
    prev_data = pd.DataFrame(sheet.get_all_records())
    prev_ltp = dict(zip(prev_data["Symbol"], prev_data["LTP"]))
except:
    prev_ltp = {}

# Read Zerodha token
try:
    token_sheet = client.open("ZerodhaTokenStore").sheet1
    tokens = token_sheet.get_all_values()[0]
    api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]
    kite = get_kite(api_key, access_token)
except:
    kite = None  # Will switch to offline mode if token is invalid

def analyze_stock(symbol):
    row = {"Symbol": symbol}
    row["LTP"] = prev_ltp.get(symbol, 0)
    try:
        daily = get_stock_data(kite, symbol, "day", 90) if kite else pd.DataFrame()
        if not daily.empty:
            row["LTP"] = daily["close"].iloc[-1]
            row["% Change"] = ((row["LTP"] - daily["close"].iloc[-2]) / daily["close"].iloc[-2]) * 100
            row["% Change"] = round(row["% Change"], 2)
            scores_1d = calculate_scores(daily)
            row["1d TMV Score"] = round(scores_1d["TMV Score"], 2)
            row["1d Trend Direction"] = scores_1d["Trend Direction"]
            row["1d Reversal Probability"] = round(scores_1d["Reversal Probability"], 2)
        else:
            row["% Change"] = 0
            row["1d TMV Score"] = 0
            row["1d Trend Direction"] = "Neutral"
            row["1d Reversal Probability"] = 0
    except Exception as e:
        print(f"⚠️ {symbol} analysis failed: {e}")
        row.update({
            "% Change": 0,
            "1d TMV Score": 0,
            "1d Trend Direction": "Neutral",
            "1d Reversal Probability": 0
        })
    return row

# Run analysis
results = []
for symbol in symbols:
    results.append(analyze_stock(symbol))

# Upload results
final_df = pd.DataFrame(results)
sheet.update([final_df.columns.tolist()] + final_df.values.tolist())
print("✅ Background analysis uploaded to Google Sheet.")
