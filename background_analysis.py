import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
import warnings

# === Safe header structure ===
EXPECTED_HEADERS = [
    "Symbol", "LTP", "% Change",
    "15m TMV Score", "15m Trend Direction", "15m Reversal Probability",
    "1d TMV Score", "1d Trend Direction", "1d Reversal Probability"
]

# === Authenticate Google Sheets ===
def authenticate_gsheets():
    try:
        import streamlit as st
        creds_dict = st.secrets["gcp_service_account"]
    except Exception:
        import toml
        creds_dict = toml.load(".streamlit/secrets.toml")["gcp_service_account"]

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(credentials)

# === Append row safely ===
def append_row_to_background_analysis_store(row_data):
    try:
        gc = authenticate_gsheets()
        sh = gc.open("BackgroundAnalysisStore")
        ws = sh.worksheet("LiveScores")

        # Validate headers
        actual_headers = ws.row_values(1)
        if actual_headers != EXPECTED_HEADERS:
            raise ValueError("Google Sheet headers do not match expected format. Please fix them manually.")

        # Prepare row values in exact order
        row = [row_data.get(col, "") for col in EXPECTED_HEADERS]
        ws.append_row(row, value_input_option="USER_ENTERED")

        print("✅ Row appended successfully.")
    except Exception as e:
        print(f"❌ Failed to append row to Google Sheet: {e}")

# === Example logic to generate dummy row ===
def run_background_analysis():
    now = datetime.now().strftime("%H:%M:%S")
    dummy_row = {
        "Symbol": "NIFTY",
        "LTP": 22200.45,
        "% Change": 0.42,
        "15m TMV Score": 78,
        "15m Trend Direction": "Uptrend",
        "15m Reversal Probability": "Low",
        "1d TMV Score": 65,
        "1d Trend Direction": "Neutral",
        "1d Reversal Probability": "Medium"
    }
    append_row_to_background_analysis_store(dummy_row)

if __name__ == "__main__":
    run_background_analysis()
