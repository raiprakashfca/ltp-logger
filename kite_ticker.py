import time
import json
import gspread
from kiteconnect import KiteConnect
from oauth2client.service_account import ServiceAccountCredentials

# STEP 1: Google Sheet Auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
with open("zerodhatokensaver-1b53153ffd25.json") as f:
    creds_dict = json.load(f)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# STEP 2: Read API key, secret, token from ZerodhaTokenStore
sheet = client.open("ZerodhaTokenStore").sheet1
tokens = sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# STEP 3: Define stock list (NIFTY 50)
symbols = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO",
    "BAJAJFINSV", "BPCL", "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB",
    "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
    "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL", "KOTAKBANK",
    "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE",
    "SBIN", "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN",
    "UPL", "ULTRACEMCO", "WIPRO"
]

# STEP 4: Map symbols to instrument tokens
print("⏳ Fetching instrument tokens...")
instrument_map = {}
for inst in kite.instruments("NSE"):
    if inst["tradingsymbol"] in symbols and inst["segment"] == "NSE":
        instrument_map[inst["tradingsymbol"]] = inst["instrument_token"]

print("✅ Token mapping complete. Starting live LTP fetch...")

# STEP 5: Start loop
while True:
    try:
        quote = kite.ltp([f"NSE:{s}" for s in symbols])
        data = []
        for symbol in symbols:
            try:
                ltp = quote[f"NSE:{symbol}"]["last_price"]
                prev_close = quote[f"NSE:{symbol}"]["ohlc"]["close"]
                pct_change = round(((ltp - prev_close) / prev_close) * 100, 2)
                data.append([symbol, round(ltp, 2), pct_change])
            except:
                data.append([symbol, 0.0, 0.0])

        sheet = client.open("LiveLTPStore").sheet1
        sheet.clear()
        sheet.append_row(["Symbol", "LTP", "% Change"])
        sheet.append_rows(data)

        print("✅ Logged LTP data to LiveLTPStore")
        time.sleep(60)

    except Exception as e:
        print("❌ Error while logging LTP:", e)
        time.sleep(60)
