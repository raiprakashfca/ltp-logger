import os
import json
import time
import logging
import pandas as pd
from kiteconnect import KiteConnect, KiteTicker
from google.oauth2.service_account import Credentials
import gspread

# --- Load credentials from environment variable ---
creds_dict = json.loads(os.environ["GSPREAD_CREDENTIALS_JSON"])

# --- Setup Google Sheets client ---
scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(credentials)

# --- Sheet setup ---
sheet = gc.open("LiveLTPStore").sheet1

# --- Zerodha API credentials ---
api_key = os.environ["Z_API_KEY"]
access_token = os.environ["Z_ACCESS_TOKEN"]

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)
kws = KiteTicker(api_key, access_token)

# --- Stocks to track ---
symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN"]
instruments = kite.instruments()
symbol_to_token = {
    s['tradingsymbol']: s['instrument_token']
    for s in instruments
    if s['tradingsymbol'] in symbols and s['exchange'] == 'NSE'
}
tokens = list(symbol_to_token.values())

ltp_data = {}

# --- Callback ---
def on_ticks(ws, ticks):
    for tick in ticks:
        for sym, tok in symbol_to_token.items():
            if tick['instrument_token'] == tok:
                ltp_data[sym] = tick['last_price']
    update_google_sheet()

def on_connect(ws, response):
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_LTP, tokens)

def update_google_sheet():
    try:
        rows = [[sym, ltp_data.get(sym, "")] for sym in symbols]
        sheet.clear()
        sheet.update([["Symbol", "LTP"]] + rows)
        print("✅ Sheet updated")
    except Exception as e:
        logging.error(f"Sheet update failed: {e}")

# --- Start ticker ---
kws.on_ticks = on_ticks
kws.on_connect = on_connect

print("📡 Starting WebSocket...")
kws.connect(threaded=True)

while True:
    time.sleep(30)
