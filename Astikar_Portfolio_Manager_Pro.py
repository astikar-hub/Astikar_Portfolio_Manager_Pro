def main():
    print("Starting portfolio engine...")

    nav_df = load_or_create_csv(NAV_FILE, ["Date", "NAV"])
    portfolio_df = load_or_create_csv(PORTFOLIO_FILE, ["Symbol", "Shares"])
    trades_df = load_or_create_csv(TRADES_FILE, ["Date", "Symbol", "Action", "Price", "Shares", "Cost"])

    today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")

    universe = get_universe()
    close_data = download_data(universe)

    latest_prices = close_data.iloc[-1].to_dict()

    # =========================
    # DATA VALIDATION GUARD
    # =========================

    valid_symbols = close_data.columns.tolist()
    coverage_ratio = len(valid_symbols) / len(universe)

    if coverage_ratio < MIN_DATA_COVERAGE:
        warning_msg = f"""
⚠️ <b>Data Validation Failed</b>

Only {coverage_ratio:.0%} of symbols returned valid data.
Rebalance aborted for safety.
"""
        send_telegram_message(warning_msg)
        print("Data validation failed. Aborting rebalance.")
        return

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
