import time
import json
import pandas as pd
import gspread
from kiteconnect import KiteConnect, KiteTicker
from oauth2client.service_account import ServiceAccountCredentials

# Step 1: Authorize Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
with open("zerodhatokensaver-1b53153ffd25.json") as f:
    creds_dict = json.load(f)

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Step 2: Get API Key, Secret, Token from ZerodhaTokenStore
token_sheet = client.open("ZerodhaTokenStore").sheet1
api_key, api_secret, access_token = token_sheet.row_values(1)

kite = KiteConnect(api_key=api_key)

# Step 3: Check Access Token
try:
    kite.set_access_token(access_token)
    kite.profile()
    print("✅ Access token is valid.")
except:
    print("\n🚨 Access token is invalid or expired. Please follow these steps:\n")
    login_url = kite.login_url()
    print(f"👉 1. Open this URL in your browser:\n\n{login_url}\n")
    request_token = input("👉 2. Paste the request token here: ").strip()
    try:
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        token_sheet.update("C1", access_token)
        kite.set_access_token(access_token)
        print("✅ Access token updated and stored in Google Sheet.")
    except Exception as e:
        print(f"❌ Failed to generate access token: {e}")
        exit()

# Step 4: Get tokens of NIFTY 50 stocks
ltp_sheet = client.open("LiveLTPStore").sheet1
symbols = [row[0] for row in ltp_sheet.get_all_values()[1:] if row]
instruments = kite.ltp(symbols)
tokens = [instruments[sym]["instrument_token"] for sym in instruments]

# Step 5: Initialize KiteTicker
kws = KiteTicker(api_key, access_token)

ltp_map = {}

def on_ticks(ws, ticks):
    global ltp_map
    for tick in ticks:
        token = tick["instrument_token"]
        price = tick.get("last_price")
        for sym, data in instruments.items():
            if data["instrument_token"] == token:
                ltp_map[sym] = price

    # Update sheet
    rows = [["Symbol", "LTP"]]
    for sym in symbols:
        ltp = ltp_map.get(sym, "")
        rows.append([sym, ltp])

    ltp_sheet.clear()
    ltp_sheet.update("A1", rows)
    print("✅ Sheet updated with latest prices.")

def on_connect(ws, response):
    print("✅ Connected. Subscribing to instruments...")
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_LTP, tokens)

def on_close(ws, code, reason):
    print(f"🔌 Disconnected: {reason}")

def on_error(ws, code, reason):
    print(f"❌ Error: {reason}")

# Step 6: Start Ticker
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close
kws.on_error = on_error

print("🚀 Starting KiteTicker...\n")
kws.connect(threaded=True)

# Keep alive
while True:
    time.sleep(30)
