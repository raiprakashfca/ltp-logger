import pandas as pd
import numpy as np
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# === Google Sheet Setup ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("zerodhatokensaver-1b53153ffd25.json", scope)
client = gspread.authorize(creds)

output_sheet = client.open("BackgroundAnalysisStore").sheet1

# === Static Symbol List (NIFTY 50 without HDFC) ===
nifty_symbols = [
    "RELIANCE", "INFY", "TCS", "ICICIBANK", "HDFCBANK", "SBIN", "BHARTIARTL", "AXISBANK", "LT",
    "ITC", "KOTAKBANK", "HINDUNILVR", "BAJFINANCE", "ASIANPAINT", "MARUTI", "NTPC", "SUNPHARMA",
    "POWERGRID", "TITAN", "HCLTECH", "ULTRACEMCO", "WIPRO", "BAJAJFINSV", "ONGC", "TECHM",
    "JSWSTEEL", "COALINDIA", "TATAMOTORS", "HINDALCO", "ADANIENT", "ADANIPORTS", "GRASIM",
    "BRITANNIA", "DIVISLAB", "EICHERMOT", "NESTLEIND", "DRREDDY", "CIPLA", "M&M", "BPCL",
    "SBILIFE", "HEROMOTOCO", "BAJAJ-AUTO", "TATASTEEL", "INDUSINDBK", "APOLLOHOSP", "HDFCLIFE",
    "UPL"
]

# === Helper to simulate offline calculations ===
def compute_tmv_score(df):
    ema_short = df['close'].ewm(span=8, adjust=False).mean().iloc[-1]
    ema_long = df['close'].ewm(span=21, adjust=False).mean().iloc[-1]
    price = df['close'].iloc[-1]

    score = np.clip((price - ema_long) / (ema_long + 1e-6), -1, 1)
    tmv_score = round((score + 1) / 2, 2)

    direction = "Bullish" if price > ema_long else "Bearish" if price < ema_short else "Neutral"
    reversal = round(np.abs(ema_short - ema_long) / (price + 1e-6), 2)

    return tmv_score, direction, reversal

# === Fallback: Simulated OHLCV Fetch ===
def fetch_ohlcv(symbol, interval="day", days=90):
    np.random.seed(hash(symbol) % 123456)  # ensure consistent output
    price = np.random.uniform(100, 3000)
    df = pd.DataFrame({
        "close": price + np.random.randn(days).cumsum()
    })
    return df

# === Build Output Table ===
output = []

for symbol in nifty_symbols:
    try:
        # Simulate 15m data
        df_15m = fetch_ohlcv(symbol, "15minute", 32)
        tmv_15m, dir_15m, rev_15m = compute_tmv_score(df_15m)

        # Simulate 1d data
        df_1d = fetch_ohlcv(symbol, "day", 90)
        tmv_1d, dir_1d, rev_1d = compute_tmv_score(df_1d)

        ltp = df_1d['close'].iloc[-1]
        prev_close = df_1d['close'].iloc[-2]
        pct_change = round(((ltp - prev_close) / prev_close) * 100, 2)

        output.append([
            symbol,
            round(ltp, 2),
            f"{pct_change}%",
            tmv_15m,
            dir_15m,
            rev_15m,
            tmv_1d,
            dir_1d,
            rev_1d
        ])

    except Exception as e:
        print(f"⚠️ {symbol} failed: {e}")

# === Write to Sheet ===
headers = [
    "Symbol", "LTP", "% Change",
    "15m TMV Score", "15m Trend Direction", "15m Reversal Probability",
    "1d TMV Score", "1d Trend Direction", "1d Reversal Probability"
]

output_sheet.clear()
output_sheet.update([headers] + output)

print("✅ Analysis complete and written to BackgroundAnalysisStore")
