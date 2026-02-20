import os
import pandas as pd
import numpy as np
import yfinance as yf
import requests
from datetime import datetime
import pytz

# =========================
# CONFIGURATION
# =========================

INITIAL_CAPITAL = 50000
MAX_STOCKS = 5
BROKERAGE_RATE = 0.001  # 0.1%
MIN_CASH_BUFFER = 0.10
TIMEZONE = "Asia/Kolkata"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

NAV_FILE = "nav_history.csv"
PORTFOLIO_FILE = "portfolio_history.csv"
TRADES_FILE = "trades_log.csv"
UNIVERSE_FILE = "nifty200.csv"

# =========================
# UTILITY FUNCTIONS
# =========================

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials missing.")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    requests.post(url, data=payload)

def is_friday():
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    return now.weekday() == 4  # Friday

def load_or_create_csv(file, columns):
    if os.path.exists(file):
        return pd.read_csv(file)
    else:
        df = pd.DataFrame(columns=columns)
        df.to_csv(file, index=False)
        return df

# =========================
# DATA FUNCTIONS
# =========================

def download_data(symbols):
    data = yf.download(symbols, period="1y", interval="1d", auto_adjust=True, progress=False)
    return data["Close"]

def calculate_momentum(close):
    momentum_3m = close.pct_change(63)
    momentum_6m = close.pct_change(126)
    return momentum_3m.iloc[-1], momentum_6m.iloc[-1]

def calculate_dma(close):
    dma_50 = close.rolling(50).mean().iloc[-1]
    dma_200 = close.rolling(200).mean().iloc[-1]
    return dma_50, dma_200

# =========================
# PORTFOLIO LOGIC
# =========================

def get_universe():
    df = pd.read_csv(UNIVERSE_FILE)
    return df['Symbol'].tolist()

def select_stocks(symbols):
    selected = []
    close_data = download_data(symbols)

    for symbol in symbols:
        try:
            close = close_data[symbol].dropna()
            if len(close) < 200:
                continue

            m3, m6 = calculate_momentum(close)
            dma50, dma200 = calculate_dma(close)
            price = close.iloc[-1]

            if (
                price > dma200 and
                dma50 > dma200 and
                m3 > 0 and
                m6 > 0
            ):
                score = m3 + m6
                selected.append((symbol, score))
        except:
            continue

    selected.sort(key=lambda x: x[1], reverse=True)
    return [s[0] for s in selected[:MAX_STOCKS]]

def calculate_portfolio_value(portfolio, close_prices):
    total = 0
    for symbol, shares in portfolio.items():
        if symbol in close_prices:
            total += shares * close_prices[symbol]
    return total

# =========================
# MAIN EXECUTION
# =========================

def main():
    print("Starting portfolio engine...")

    nav_df = load_or_create_csv(NAV_FILE, ["Date", "NAV"])
    portfolio_df = load_or_create_csv(PORTFOLIO_FILE, ["Symbol", "Shares"])
    trades_df = load_or_create_csv(TRADES_FILE, ["Date", "Symbol", "Action", "Price", "Shares", "Cost"])

    today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")

    universe = get_universe()
    close_data = download_data(universe)

    latest_prices = close_data.iloc[-1].to_dict()

    # Load portfolio
    portfolio = {}
    if not portfolio_df.empty:
        for _, row in portfolio_df.iterrows():
            portfolio[row["Symbol"]] = row["Shares"]

    # If first run
    if nav_df.empty:
        capital = INITIAL_CAPITAL
    else:
        capital = nav_df.iloc[-1]["NAV"]

    # Calculate current value
    invested_value = calculate_portfolio_value(portfolio, latest_prices)
    cash = capital - invested_value

    message_lines = []
    message_lines.append(f"📊 <b>Astikar Fund Weekly Report</b>")
    message_lines.append(f"Date: {today}")

    if is_friday():
        message_lines.append("🔁 Rebalance executed.")

        selected = select_stocks(universe)

        # SELL stocks not in selected
        for symbol in list(portfolio.keys()):
            if symbol not in selected:
                price = latest_prices.get(symbol, 0)
                shares = portfolio[symbol]
                proceeds = shares * price
                cost = proceeds * BROKERAGE_RATE
                cash += proceeds - cost

                trades_df.loc[len(trades_df)] = [
                    today, symbol, "SELL", price, shares, cost
                ]

                del portfolio[symbol]

        # BUY new stocks
        allocation = capital * (1 - MIN_CASH_BUFFER) / MAX_STOCKS

        for symbol in selected:
            if symbol not in portfolio:
                price = latest_prices.get(symbol, 0)
                if price == 0:
                    continue

                shares = int(allocation / price)
                cost = shares * price * BROKERAGE_RATE
                total_cost = shares * price + cost

                if shares > 0 and cash >= total_cost:
                    cash -= total_cost
                    portfolio[symbol] = shares

                    trades_df.loc[len(trades_df)] = [
                        today, symbol, "BUY", price, shares, cost
                    ]

    else:
        message_lines.append("ℹ️ Not Friday. No rebalance.")

    # Recalculate portfolio value
    invested_value = calculate_portfolio_value(portfolio, latest_prices)
    nav = invested_value + cash

    # Save NAV
    nav_df.loc[len(nav_df)] = [today, nav]
    nav_df.to_csv(NAV_FILE, index=False)

    # Save portfolio
    portfolio_df = pd.DataFrame([
        {"Symbol": s, "Shares": sh} for s, sh in portfolio.items()
    ])
    portfolio_df.to_csv(PORTFOLIO_FILE, index=False)

    # Save trades
    trades_df.to_csv(TRADES_FILE, index=False)

    # Reporting
    total_return = ((nav - INITIAL_CAPITAL) / INITIAL_CAPITAL) * 100
    message_lines.append(f"💰 NAV: ₹{nav:,.2f}")
    message_lines.append(f"📈 Total Return: {total_return:.2f}%")
    message_lines.append(f"💵 Cash: ₹{cash:,.2f}")
    message_lines.append(f"📊 Holdings: {len(portfolio)} stocks")

    send_telegram_message("\n".join(message_lines))

    print("Execution complete.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        
        error_details = traceback.format_exc()
        error_message = f"""
❌ <b>Astikar Fund Engine Error</b>

Time: {datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S")}

Error:
{str(e)}

Details:
{error_details}
"""
        send_telegram_message(error_message)
        print("Error occurred. Telegram alert sent.")
        raise

