import os
import json
import base64
import time
import logging
import pandas as pd
from kiteconnect import KiteConnect, KiteTicker
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Set up logging
logging.basicConfig(level=logging.INFO)

# Decode Google Service Credentials from environment variable
creds_base64 = os.environ.get("GSPREAD_CREDENTIALS_JSON")
if not creds_base64:
    raise ValueError("❌ GSPREAD_CREDENTIALS_JSON not found in environment variables.")

creds_json = base64.b64decode(creds_base64).decode("utf-8")
creds_dict = json.loads(creds_json)

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(credentials)

# Load tokens from Google Sheet
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0][:3]  # Only use first 3 values
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Read list of symbols
ltp_sheet = client.open("LiveLTPStore").sheet1
symbols = [row[0] for row in ltp_sheet.get_all_values()[1:] if row]  # Ignore header

# Fetch instrument tokens for symbols
instruments = pd.DataFrame(kite.instruments("NSE"))
symbol_token_map = {}
for symbol in symbols:
    match = instruments[instruments["tradingsymbol"] == symbol]
    if not match.empty:
        symbol_token_map[symbol] = int(match.iloc[0]["instrument_token"])

if not symbol_token_map:
    raise ValueError("❌ No valid symbols found for LTP tracking.")

tokens = list(symbol_token_map.values())

# Initialize KiteTicker
kws = KiteTicker(api_key, access_token)

# Setup LTP cache
latest_ltps = {}

def on_ticks(ws, ticks):
    for tick in ticks:
        for sym, token in symbol_token_map.items():
            if tick["instrument_token"] == token and "last_price" in tick:
                latest_ltps[sym] = tick["last_price"]

    if latest_ltps:
        try:
            rows = [[symbol, latest_ltps.get(symbol, "")] for symbol in symbols]
            ltp_sheet.update(values=[["Symbol", "LTP"]], range_name="A1:B1")  # ✅ Updated
            ltp_sheet.update(values=rows, range_name="A2")                    # ✅ Updated
            logging.info("✅ LTPs updated to Google Sheet.")
        except Exception as e:
            logging.warning(f"⚠️ Sheet update failed: {e}")

def on_connect(ws, response):
    ws.subscribe(tokens)
    logging.info("✅ Subscribed to tokens.")

def on_close(ws, code, reason):
    logging.warning("⚠️ Connection closed: %s", reason)

def on_error(ws, code, reason):
    logging.error("❌ WebSocket error: %s", reason)

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close
kws.on_error = on_error

logging.info("🚀 Starting Kite Ticker WebSocket...")
kws.connect(threaded=True)

# Run for 6 hours
for _ in range(6 * 60):  # Every 60 seconds for 6 hours
    time.sleep(60)

kws.close()
logging.info("🛑 Kite Ticker stopped after session.")
