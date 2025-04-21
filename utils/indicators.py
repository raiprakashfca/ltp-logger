import pandas as pd
import numpy as np
import pandas_ta as ta

def calculate_scores(df):
    scores = {}

    # Ensure timestamp is datetime and set as index
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # Coerce important price columns to numeric
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows with NaNs in essential columns
    df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])

    if df.empty:
        return {}

    # Trend Indicators
    ema8 = ta.ema(df['close'], length=8)
    ema21 = ta.ema(df['close'], length=21)
    supertrend = ta.supertrend(df['high'], df['low'], df['close'])["SUPERT_7_3.0"]

    trend_score = (
        ((ema8 > ema21).astype(int) + (df['close'] > supertrend).astype(int)) / 2
    ).astype(float)

    # Momentum Indicators
    macd = ta.macd(df['close'])
    rsi = ta.rsi(df['close'])
    adx = ta.adx(df['high'], df['low'], df['close'])

    momentum_score = (
        (macd['MACD_12_26_9'] > macd['MACDs_12_26_9']).astype(int) +
        (rsi > 50).astype(int) +
        (adx['ADX_14'] > 20).astype(int)
    ) / 3
    momentum_score = momentum_score.astype(float)

    # Volume Indicators
    obv = ta.obv(df['close'], df['volume'])
    mfi = ta.mfi(df['high'], df['low'], df['close'], df['volume'])

    volume_score = (
        (obv.diff() > 0).astype(int) + (mfi > 50).astype(int)
    ) / 2
    volume_score = volume_score.astype(float)

    # Final scores
    weights = {
        'Trend Score': 0.4,
        'Momentum Score': 0.35,
        'Volume Score': 0.25
    }

    scores['Trend Score'] = trend_score.iloc[-1]
    scores['Momentum Score'] = momentum_score.iloc[-1]
    scores['Volume Score'] = volume_score.iloc[-1]
    scores['TMV Score'] = (
        scores['Trend Score'] * weights['Trend Score'] +
        scores['Momentum Score'] * weights['Momentum Score'] +
        scores['Volume Score'] * weights['Volume Score']
    )

    # Direction
    if scores['Trend Score'] >= 0.75:
        scores['Trend Direction'] = 'Bullish'
    elif scores['Trend Score'] <= 0.25:
        scores['Trend Direction'] = 'Bearish'
    else:
        scores['Trend Direction'] = 'Neutral'

    # Reversal Probability
    recent_rsi = rsi.tail(5)
    if recent_rsi.isnull().all():
        scores['Reversal Probability'] = 0.0
    else:
        reversal_prob = ((recent_rsi < 30) | (recent_rsi > 70)).sum() / len(recent_rsi)
        scores['Reversal Probability'] = float(reversal_prob)

    return scores
