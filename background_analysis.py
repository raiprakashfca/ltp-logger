import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from kiteconnect import KiteConnect
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores

# Authorize Google Sheets access
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
with open("zerodhatokensaver-1b53153ffd25.json") as f:
    creds_dict = json.load(f)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Read Zerodha token
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

kite = get_kite(api_key, access_token)

# Read list of symbols from LTP sheet
ltp_sheet = client.open("LiveLTPStore").sheet1
ltp_data = pd.DataFrame(ltp_sheet.get_all_records())
symbols = ltp_data["Symbol"].tolist()

TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5},
    "1d": {"interval": "day", "days": 90},
}

all_data = []

for symbol in symbols:
    row = {"Symbol": symbol}
    live_row = ltp_data[ltp_data["Symbol"] == symbol]
    if not live_row.empty:
        row["LTP"] = float(live_row.iloc[0]["LTP"])
        row["Close"] = float(live_row.iloc[0].get("Prev Close", 0))
        row["% Change"] = round(((row["LTP"] - row["Close"]) / row["Close"]) * 100, 2) if row["Close"] else 0

    for label, config in TIMEFRAMES.items():
        df = get_stock_data(kite, symbol, config["interval"], config["days"])
        if not df.empty:
            try:
                result = calculate_scores(df)
                for key, value in result.items():
                    adjusted_key = "TMV Score" if key == "Total Score" else key
                    row[f"{label} | {adjusted_key}"] = value
            except Exception as e:
                print(f"Error processing {symbol} [{label}]: {e}")
    all_data.append(row)

# Upload to Google Sheet
result_df = pd.DataFrame(all_data)
try:
    output_sheet = client.open("Stock Rankings").worksheet("Precomputed")
    output_sheet.clear()
    output_sheet.update([result_df.columns.values.tolist()] + result_df.values.tolist())
    print("✅ Precomputed scores updated to Google Sheet.")
except Exception as e:
    print(f"❌ Sheet update failed: {e}")