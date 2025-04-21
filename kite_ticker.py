import os
import json
import time
import gspread
from kiteconnect import KiteConnect, KiteTicker
from oauth2client.service_account import ServiceAccountCredentials

# Load credentials from environment variable
creds_dict = json.loads(os.environ["GSPREAD_SERVICE_ACCOUNT"])
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Zerodha credentials from Google Sheet
sheet = client.open("ZerodhaTokenStore").sheet1
tokens = sheet.get_all_values()[0]
api_key = tokens[0]
access_token = tokens[2]

# Setup Kite Connect and Ticker
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)
kws = KiteTicker(api_key, access_token)

symbols = [
    "RELIANCE", "INFY", "TCS", "ICICIBANK", "HDFCBANK", "SBIN",
    "BHARTIARTL", "WIPRO", "AXISBANK", "LT", "MARUTI", "NESTLEIND"
]

# Fetch instrument tokens
instruments = kite.instruments("NSE")
symbol_token_map = {}
for inst in instruments:
    if inst["tradingsymbol"] in symbols:
        symbol_token_map[inst["instrument_token"]] = inst["tradingsymbol"]

live_prices = {}

def on_ticks(ws, ticks):
    for tick in ticks:
        token = tick["instrument_token"]
        symbol = symbol_token_map.get(token)
        if symbol:
            ltp = tick["last_price"]
            live_prices[symbol] = ltp

def on_connect(ws, response):
    print("✅ WebSocket connected.")
    ws.subscribe(list(symbol_token_map.keys()))

def on_close(ws, code, reason):
    print("❌ WebSocket disconnected.", code, reason)

def log_to_sheet():
    try:
        sheet = client.open("LiveLTPStore").sheet1
        sheet.clear()
        data = [["Symbol", "LTP", "% Change"]]
        for symbol, ltp in live_prices.items():
            change = 0  # Placeholder: If you have previous day close, calculate change
            data.append([symbol, ltp, change])
        sheet.update("A1", data)
        print("📊 LTPs logged.")
    except Exception as e:
        print("⚠️ Error updating sheet:", e)

# Attach handlers
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Start ticker in background
import threading
threading.Thread(target=kws.connect, daemon=True).start()

# Periodic sheet update loop
while True:
    time.sleep(15)
    if live_prices:
        log_to_sheet()
