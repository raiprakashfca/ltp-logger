import pandas as pd
import json
import datetime
from kiteconnect import KiteConnect
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# Load Google Sheet credentials
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
with open("zerodhatokensaver-1b53153ffd25.json") as f:
    creds_dict = json.load(f)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Zerodha tokens
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0][:3]
api_key, api_secret, access_token = tokens

kite = get_kite(api_key, access_token)

# Detect offline mode based on NSE market status
market_open = datetime.datetime.now().time() < datetime.time(15, 30)

# Use historical to_date if market is closed
today = datetime.date.today()
to_date = today if market_open else today - datetime.timedelta(days=1)

TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5, "from": to_date - datetime.timedelta(days=5)},
    "1d": {"interval": "day", "days": 90, "from": to_date - datetime.timedelta(days=90)},
}

symbols = [
    "RELIANCE", "INFY", "TCS", "ICICIBANK", "HDFCBANK", "SBIN", "BHARTIARTL", "LT", "AXISBANK", "ITC",
    "KOTAKBANK", "HINDUNILVR", "BAJFINANCE", "WIPRO", "ASIANPAINT", "TECHM", "HCLTECH", "MARUTI",
    "TITAN", "ULTRACEMCO", "SUNPHARMA", "NESTLEIND", "POWERGRID", "JSWSTEEL", "TATAMOTORS",
    "ADANIENT", "ADANIPORTS", "CIPLA", "DIVISLAB", "NTPC", "BPCL", "BAJAJFINSV", "BAJAJ-AUTO",
    "GRASIM", "HEROMOTOCO", "COALINDIA", "BRITANNIA", "HINDALCO", "EICHERMOT", "SBILIFE", "ONGC",
    "UPL", "TATASTEEL", "INDUSINDBK", "ICICIPRULI", "DRREDDY", "HDFCLIFE"
]

output_rows = []

for symbol in symbols:
    row = {"Symbol": symbol}
    for tf, cfg in TIMEFRAMES.items():
        df = get_stock_data(kite, symbol, cfg["interval"], cfg["days"], from_date=cfg["from"], to_date=to_date)
        if not df.empty:
            try:
                result = calculate_scores(df)
                row[f"{tf} TMV Score"] = round(result["Total Score"], 2)
                row[f"{tf} Trend Direction"] = result["Trend Direction"]
                row[f"{tf} Reversal Probability"] = round(result["Reversal Probability"], 2)
                if tf == "1d":
                    row["LTP"] = float(df["close"].iloc[-1])
                    row["% Change"] = round(((df["close"].iloc[-1] - df["close"].iloc[-2]) / df["close"].iloc[-2]) * 100, 2)
            except Exception as e:
                print(f"❌ {symbol} ({tf}) error: {e}")
    output_rows.append(row)

final_df = pd.DataFrame(output_rows)

# Push to sheet
sheet = client.open("BackgroundAnalysisStore").sheet1
sheet.clear()
sheet.update(
    [final_df.columns.values.tolist()] + final_df.values.tolist()
)
print("✅ Sheet updated.")
