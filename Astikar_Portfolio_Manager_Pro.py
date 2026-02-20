# ============================================
# ASTIKAR FUND SYSTEM - WEEKLY PRO ENGINE
# Weekly Rebalance + Pyramiding + Telegram
# ============================================

import os
import sys
import pandas as pd
import numpy as np
import yfinance as yf
import requests
from datetime import datetime

# ================= CONFIG =================
UNIVERSE_FILE = "nifty200.csv"
SECTOR_FILE = "sector_mapping.csv"
POSITIONS_FILE = "portfolio_positions.csv"

INDEX_SYMBOL = "^NSEI"
CAPITAL = 50000
TOP_N = 10

MAX_ADDS = 2
MAX_POSITION_MULTIPLIER = 2.0

REGIME_MA = 200
CRASH_THRESHOLD = -0.12

TELEGRAM_BOT_TOKEN = "PUT_YOUR_NEW_TOKEN_HERE"
TELEGRAM_CHAT_ID = "8144938221"

# ================= UTILITIES =================

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": message}

    try:
        resp = requests.post(url, data=data)
        print("Telegram:", resp.text)
    except Exception as e:
        print("Telegram error:", e)

def safe_read_csv(filepath):
    try:
        return pd.read_csv(filepath, encoding="utf-8")
    except:
        return pd.read_csv(filepath, encoding="latin1")

# ================= LOADERS =================

def load_universe():
    df = safe_read_csv(UNIVERSE_FILE)
    tickers = df.iloc[:, 0].dropna().astype(str).tolist()
    return [t if t.endswith(".NS") else t + ".NS" for t in tickers]

def load_sector_mapping():
    if not os.path.exists(SECTOR_FILE):
        return {}
    df = safe_read_csv(SECTOR_FILE)
    df["Ticker"] = df["Symbol"].apply(lambda x: x if x.endswith(".NS") else x + ".NS")
    return dict(zip(df["Ticker"], df["Sector"]))

def load_positions():
    if not os.path.exists(POSITIONS_FILE):
        return pd.DataFrame(columns=["Ticker","Shares","Avg_Cost","Adds"])

    df = safe_read_csv(POSITIONS_FILE)

    df = df.dropna(subset=["Ticker"])
    df["Ticker"] = df["Ticker"].astype(str).str.strip()
    df = df[df["Ticker"] != ""]
    df = df[df["Ticker"].str.lower() != "nan"]

    return df.reset_index(drop=True)

def save_positions(df):
    df.to_csv(POSITIONS_FILE, index=False)

# ================= FILTERS =================

def extract_close(data):
    if isinstance(data.columns, pd.MultiIndex):
        data = data["Close"]
    return data

def regime_filter():
    raw = yf.download(INDEX_SYMBOL, period="1y", auto_adjust=True, progress=False)
    index = raw["Close"]
    ma = index.rolling(REGIME_MA).mean()
    return float(index.iloc[-1]) > float(ma.iloc[-1])

def crash_filter():
    raw = yf.download(INDEX_SYMBOL, period="6mo", auto_adjust=True, progress=False)
    index = raw["Close"]
    ret_3m = (float(index.iloc[-1]) / float(index.iloc[-63])) - 1
    return ret_3m >= CRASH_THRESHOLD

# ================= PORTFOLIO ENGINE =================

def generate_portfolio(data):
    weekly = data.resample("W-FRI").last()
    weekly = weekly.dropna(axis=1, thresh=13)

    returns = weekly.pct_change(12)
    momentum = returns.iloc[-1].dropna().sort_values(ascending=False)

    return momentum.head(TOP_N).index.tolist()

def generate_orders(selected, price_data):

    sector_map = load_sector_mapping()
    positions = load_positions()

    prev_portfolio = positions["Ticker"].tolist()
    sell_list = list(set(prev_portfolio) - set(selected))

    orders = []
    base_allocation = CAPITAL / TOP_N

    # SELL
    for ticker in sell_list:
        sector = sector_map.get(ticker,"Unknown")
        orders.append(("SELL_ALL", ticker, sector, 0, 0, 0))
        positions = positions[positions["Ticker"] != ticker]

    # BUY / ADD
    for ticker in selected:

        price = float(price_data[ticker].iloc[-1])
        sector = sector_map.get(ticker,"Unknown")
        existing = positions[positions["Ticker"] == ticker]

        if existing.empty:
            qty = int(base_allocation / price)
            if qty > 0:
                orders.append(("BUY", ticker, sector, qty, price, qty*price))
                new_row = pd.DataFrame([{
                    "Ticker": ticker,
                    "Shares": qty,
                    "Avg_Cost": price,
                    "Adds": 0
                }])
                positions = pd.concat([positions, new_row], ignore_index=True)

        else:
            shares = existing["Shares"].values[0]
            avg_cost = existing["Avg_Cost"].values[0]
            adds = existing["Adds"].values[0]

            max_value = base_allocation * MAX_POSITION_MULTIPLIER
            current_value = shares * price

            if adds < MAX_ADDS and price > avg_cost and current_value < max_value:
                add_qty = int(base_allocation / price)
                if add_qty > 0:
                    orders.append((f"ADD_{adds+1}", ticker, sector, add_qty, price, add_qty*price))

                    new_shares = shares + add_qty
                    new_avg = ((shares*avg_cost)+(add_qty*price))/new_shares

                    positions.loc[positions["Ticker"]==ticker,
                                  ["Shares","Avg_Cost","Adds"]] = [new_shares,new_avg,adds+1]

    return orders, positions

# ================= MAIN =================

def main():

    print("ASTIKAR FUND SYSTEM RUNNING...")

    tickers = load_universe()
    data = yf.download(tickers, period="12mo", interval="1d", auto_adjust=True, progress=True)

    if isinstance(data.columns, pd.MultiIndex):
        data = data["Close"]

    if not regime_filter():
        send_telegram("📉 Weekly System: Market not bullish. No allocation.")
        return

    if not crash_filter():
        send_telegram("⚠️ Weekly System: Crash threshold triggered.")
        return

    selected = generate_portfolio(data)
    orders, positions = generate_orders(selected, data)

    total_used = sum([o[5] for o in orders])
    balance = CAPITAL - total_used

    msg = f"📊 ASTIKAR WEEKLY REBALANCE\n\nBalance: ₹{round(balance,2)}\n\n"

    for action,ticker,sector,qty,price,value in orders:
        if action == "SELL_ALL":
            msg += f"🔻 SELL {ticker}\n"
        else:
            msg += f"🟢 {action} {ticker} — {qty} @ ₹{round(price,2)}\n"

    send_telegram(msg)
    save_positions(positions)

if __name__ == "__main__":

    main()
