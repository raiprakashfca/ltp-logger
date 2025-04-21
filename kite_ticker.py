import json
import os
import time
import pandas as pd
from kiteconnect import KiteConnect, KiteTicker
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Load Zerodha credentials from Google Sheet via environment variable
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = json.loads(os.environ["GSPREAD_CREDENTIALS_JSON"])
client = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds, scope))

# Fetch tokens from ZerodhaTokenStore
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Initialize ticker
kws = KiteTicker(api_key, access_token)

# Define symbols to track
symbols = [
    "NSE:NIFTY 50", "NSE:RELIANCE", "NSE:INFY", "NSE:TCS", "NSE:ICICIBANK",
    "NSE:HDFCBANK", "NSE:SBIN", "NSE:BHARTIARTL"
]

# Get instrument tokens
instruments = kite.instruments()
token_map = {}
for sym in symbols:
    try:
        seg, tradingsym = sym.split(":")
        token = next(i["instrument_token"] for i in instruments if i["exchange"] == seg and i["tradingsymbol"] == tradingsym)
        token_map[token] = tradingsym
    except StopIteration:
        print(f"⚠️ Token not found for {sym}")

ltp_data = {}

# On tick event
def on_ticks(ws, ticks):
    global ltp_data
    for tick in ticks:
        token = tick["instrument_token"]
        tradingsym = token_map.get(token)
        if tradingsym:
            ltp = tick.get("last_price")
            ltp_data[tradingsym] = ltp
            print(f"{tradingsym}: {ltp}")

    if ltp_data:
        df = pd.DataFrame(list(ltp_data.items()), columns=["Symbol", "LTP"])
        try:
            sheet = client.open("LiveLTPStore").sheet1
            sheet.clear()
            sheet.update([df.columns.values.tolist()] + df.values.tolist())
            print("✅ Google Sheet updated with live LTPs.")
        except Exception as e:
            print(f"⚠️ Sheet update failed: {e}")

def on_connect(ws, response):
    print("✅ WebSocket connected. Subscribing to instruments...")
    ws.subscribe(list(token_map.keys()))

def on_close(ws, code, reason):
    print(f"🔌 WebSocket closed: {code} - {reason}")

# Register callbacks
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Start WebSocket
print("🚀 Starting LTP stream...")
kws.connect(threaded=False)
