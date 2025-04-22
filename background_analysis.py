
import gspread
import streamlit as st
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

def update_google_sheet(dataframe, sheet_name, worksheet_name):
    try:
        gc = gspread.service_account_from_dict(dict(st.secrets["gcp_service_account"]))
        sh = gc.open(sheet_name)
        worksheet = sh.worksheet(worksheet_name)
        worksheet.clear()
        worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
        st.success("✅ Sheet updated successfully!")
    except Exception as e:
        st.error(f"❌ Failed to update Google Sheet: {e}")

# Example usage:
if __name__ == "__main__":
    st.title("🔧 Background Analysis Debug Tool")
    df = pd.DataFrame({
        "Symbol": ["RELIANCE", "HDFCBANK"],
        "LTP": [2800.15, 1601.55],
        "% Change": [0.52, -0.41],
        "15m TMV Score": [4, 3],
        "15m Trend Direction": ["Up", "Flat"],
        "15m Reversal Probability": [0.15, 0.65],
        "1d TMV Score": [3, 2],
        "1d Trend Direction": ["Up", "Down"],
        "1d Reversal Probability": [0.2, 0.8]
    })
    update_google_sheet(df, "BackgroundAnalysisStore", "Sheet1")
