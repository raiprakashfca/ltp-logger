import json
import pandas as pd
from datetime import datetime
from utils.zerodha import get_kite, get_stock_data
from utils.indicators import calculate_scores
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Connect to Google Sheet
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(open("gspread_credentials.json").read())
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Read API tokens
token_sheet = client.open("ZerodhaTokenStore").sheet1
tokens = token_sheet.get_all_values()[0]
api_key, api_secret, access_token = tokens[0], tokens[1], tokens[2]

# Initialize Kite Connect
kite = get_kite(api_key, access_token)

# Read symbols from LiveLTPStore
symbols = client.open("LiveLTPStore").sheet1.col_values(1)[1:]  # Skip header
print(f"📦 Total symbols to analyze: {len(symbols)}")

# Timeframes to fetch
TIMEFRAMES = {
    "15m": {"interval": "15minute", "days": 5},
    "1d": {"interval": "day", "days": 90}
}

rows = []

for symbol in symbols:
    print(f"⏳ Processing {symbol}")
    row = {"Symbol": symbol}

    try:
        # Get latest LTP from LiveLTPStore
        ltp_sheet = client.open("LiveLTPStore").sheet1
        ltp_data = pd.DataFrame(ltp_sheet.get_all_records())
        ltp_row = ltp_data[ltp_data["Symbol"] == symbol]
        ltp = float(ltp_row.iloc[0]["LTP"]) if not ltp_row.empty else None
        row["LTP"] = ltp

        # Calculate % change from daily close
        daily_df = get_stock_data(kite, symbol, "day", 2)
        if not daily_df.empty:
            last_close = daily_df.iloc[-2]["close"]
            row["% Change"] = round(((ltp - last_close) / last_close) * 100, 2) if ltp else None
        else:
            row["% Change"] = None

        # Analyze both timeframes
        for tf, config in TIMEFRAMES.items():
            try:
                df = get_stock_data(kite, symbol, config["interval"], config["days"])
                if df.empty:
                    raise Exception("Empty dataframe")
                scores = calculate_scores(df)
                row[f"{tf} TMV Score"] = round(scores.get("Total Score", 0), 2)
                row[f"{tf} Trend Direction"] = scores.get("Trend Direction", "")
                row[f"{tf} Reversal Probability"] = round(scores.get("Reversal Probability", 0), 2)
            except:
                print(f"⚠️ Live data failed for {symbol} [{tf}] — using fallback")
                row[f"{tf} TMV Score"] = 0
                row[f"{tf} Trend Direction"] = "Neutral"
                row[f"{tf} Reversal Probability"] = 0

    except Exception as e:
        print(f"❌ Skipping {symbol}: {e}")
        continue

    rows.append(row)

# Write to final Google Sheet
output_sheet = client.open("BackgroundAnalysisStore").sheet1
output_sheet.clear()
headers = [
    "Symbol", "LTP", "% Change",
    "15m TMV Score", "15m Trend Direction", "15m Reversal Probability",
    "1d TMV Score", "1d Trend Direction", "1d Reversal Probability"
]
output_sheet.append_row(headers)
for row in rows:
    output_sheet.append_row([row.get(h, "") for h in headers])

print("✅ Background analysis updated successfully!")
