import os
import base64
import json
import time
import logging
import gspread
from kiteconnect import KiteConnect, KiteTicker
from oauth2client.service_account import ServiceAccountCredentials

# Setup logging
logging.basicConfig(level=logging.INFO)

# Load Google Sheet Credentials from BASE64
base64_creds = os.environ.get("GSPREAD_CREDENTIALS_JSON")
if not base64_creds:
    raise Exception("Missing GSPREAD_CREDENTIALS_JSON in environment variables")

json_creds = json.loads(base64.b64decode(base64_creds).decode("utf-8"))
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_dict(json_creds, scope)
client = gspread.authorize(credentials)

# Read API credentials from sheet
token_sheet = client.open("ZerodhaTokenStore").sheet1
api_key, api_secret, access_token = token_sheet.get_all_values()[0]

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Instruments to subscribe
symbols = [row[0] for row in client.open("LiveLTPStore").sheet1.get_all_values()[1:]]
instruments = []
all_instruments = kite.instruments("NSE")

for symbol in symbols:
    try:
        instrument_token = next(item["instrument_token"] for item in all_instruments if item["tradingsymbol"] == symbol)
        instruments.append(instrument_token)
    except StopIteration:
        logging.warning(f"Symbol {symbol} not found in instrument list")

# WebSocket LTP Ticker
ltp_sheet = client.open("LiveLTPStore").sheet1
kite_ticker = KiteTicker(api_key, access_token)

def on_ticks(ws, ticks):
    data = ltp_sheet.get_all_records()
    updated = 0
    for tick in ticks:
        for i, row in enumerate(data):
            if row["Token"] == tick["instrument_token"]:
                ltp_sheet.update_cell(i + 2, 3, tick["last_price"])  # LTP column
                updated += 1
    logging.info(f"Updated {updated} rows")

def on_connect(ws, response):
    logging.info("WebSocket connected")
    ws.subscribe(instruments)

def on_close(ws, code, reason):
    logging.warning(f"WebSocket closed: {reason}")

kite_ticker.on_ticks = on_ticks
kite_ticker.on_connect = on_connect
kite_ticker.on_close = on_close

logging.info("Starting WebSocket...")
kite_ticker.connect(threaded=True)

# Keep script alive
while True:
    time.sleep(30)
