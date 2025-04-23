
import pandas as pd
from fetch_ohlc import fetch_ohlc_data, calculate_indicators
import gspread
from google.oauth2.service_account import Credentials

# Authenticate with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"], scopes=scope
)
client = gspread.authorize(credentials)
sheet = client.open("BackgroundAnalysisStore").worksheet("Sheet1")

# Stock list including TATAPOWER
symbols = [
    "RELIANCE", "INFY", "TCS", "HDFCBANK", "ICICIBANK",
    "SBIN", "AXISBANK", "LT", "ITC", "BHARTIARTL", "TATAPOWER"
]

records = []

for symbol in symbols:
    try:
        df_15m = fetch_ohlc_data(symbol, "15minute", 3)
        df_1d = fetch_ohlc_data(symbol, "day", 30)
        indicators_15m = calculate_indicators(df_15m)
        indicators_1d = calculate_indicators(df_1d)
        ltp = df_15m['close'].iloc[-1]
        pct_change = ((df_15m['close'].iloc[-1] - df_15m['open'].iloc[-1]) / df_15m['open'].iloc[-1]) * 100

        record = {
            "Symbol": symbol,
            "LTP": round(ltp, 2),
            "% Change": round(pct_change, 2),
            "15m TMV Score": indicators_15m.get("TMV_Score", 0),
            "15m Trend Direction": indicators_15m.get("Trend", ""),
            "15m Reversal Probability": indicators_15m.get("Reversal_Prob", 0),
            "1d TMV Score": indicators_1d.get("TMV_Score", 0),
            "1d Trend Direction": indicators_1d.get("Trend", ""),
            "1d Reversal Probability": indicators_1d.get("Reversal_Prob", 0)
        }
        records.append(record)
    except Exception as e:
        print(f"Error processing {symbol}: {e}")

# Convert to DataFrame and update sheet
df_final = pd.DataFrame(records)
sheet.clear()
sheet.update([df_final.columns.values.tolist()] + df_final.values.tolist())
