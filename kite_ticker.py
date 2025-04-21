import os
import base64
import json
import time
import datetime
import pandas as pd
import gspread
from kiteconnect import KiteTicker, KiteConnect
from oauth2client.service_account import ServiceAccountCredentials

# === 1. Load Google Sheets Credentials from BASE64 env variable ===
encoded_creds = os.getenv("GSPREAD_CREDENTIALS_JSON")

if not encoded_creds:
    raise Exception("❌ GSPREAD_CREDENTIALS_JSON not found in environment variables.")

try:
    decoded_creds = base64.b64decode(encoded_creds).decode("utf-8")
    creds_dict = json.loads(decoded_creds)
except Exception as e:
    raise Exception("❌ Failed to decode GSPREAD_CREDENTIALS_JSON: " + str(e))

# === 2. Authenticate Google Sheets ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# === 3. Load Tokens ===
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens_row = token_sheet.get_all_values()[0][:3]  # Take only first 3 (ignore timestamp)
api_key, api_secret, access_token = tokens_row

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# === 4. Fetch instrument tokens for NIFTY 50 ===
nifty_50 = [
    "RELIANCE", "INFY", "TCS", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "SBIN", "AXISBANK",
    "LT", "ITC", "HINDUNILVR", "BAJFINANCE", "ASIANPAINT", "BHARTIARTL", "HCLTECH",
    "WIPRO", "MARUTI", "SUNPHARMA", "HDFC", "ADANIENT", "ADANIPORTS", "TITAN",
    "ULTRACEMCO", "TECHM", "POWERGRID", "JSWSTEEL", "COALINDIA", "NTPC", "ONGC",
    "BPCL", "GRASIM", "DIVISLAB", "BRITANNIA", "CIPLA", "TATASTEEL", "BAJAJFINSV",
    "SBILIFE", "DRREDDY", "HEROMOTOCO", "M&M", "APOLLOHOSP", "BAJAJ-AUTO", "EICHERMOT",
    "HDFCLIFE", "HINDALCO", "INDUSINDBK", "SHREECEM", "NESTLEIND", "TATAMOTORS"
]

instruments = kite.instruments("NSE")
symbol_to_token = {
    inst["tradingsymbol"]: inst["instrument_token"]
    for inst in instruments if inst["tradingsymbol"] in nifty_50
}

# === 5. Prepare KiteTicker for live streaming ===
kws = KiteTicker(api_key, access_token)

# === 6. Open target Google Sheet to write LTPs ===
sheet = client.open("LiveLTPStore").sheet1
sheet.update("A1:B1", [["Symbol", "LTP"]])  # Clear headers

# === 7. WebSocket handlers ===
ltp_map = {}

def on_ticks(ws, ticks):
    global ltp_map
    for tick in ticks:
        token = tick['instrument_token']
        for sym, tok in symbol_to_token.items():
            if tok == token:
                ltp = tick['last_price']
                ltp_map[sym] = ltp
                break

def on_connect(ws, response):
    print("✅ Connected to WebSocket.")
    ws.subscribe(list(symbol_to_token.values()))
    ws.set_mode(ws.MODE_LTP, list(symbol_to_token.values()))

def on_close(ws, code, reason):
    print(f"⚠️ WebSocket closed: {code} {reason}")

def on_error(ws, code, reason):
    print(f"❌ Error: {code}, {reason}")

# === 8. Periodic Writer to Google Sheet ===
def log_to_gsheet_every(interval=5):
    while True:
        rows = [[sym, ltp_map.get(sym, "")] for sym in nifty_50]
        sheet.update("A2", rows)
        print("✅ Updated LiveLTPStore:", datetime.datetime.now().strftime("%H:%M:%S"))
        time.sleep(interval)

# === 9. Run both WebSocket and Sheet logger ===
if __name__ == "__main__":
    from threading import Thread

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error

    writer_thread = Thread(target=log_to_gsheet_every, daemon=True)
    writer_thread.start()

    kws.connect(threaded=True)

    while True:
        time.sleep(1)
