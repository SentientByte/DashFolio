# ------------------------------
# IMPORTS
# ------------------------------
import os
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import json
import re

# ------------------------------
# CONFIG PATHS & DEFAULTS
# ------------------------------
BASE_DIR = os.path.dirname(__file__)
EXCEL_FILE = os.path.join(BASE_DIR, 'portfolio.xlsx')
PRICE_FOLDER = os.path.join(BASE_DIR, 'price_data')

# ------------------------------
# LOAD CONFIG FROM FILE
# ------------------------------
CONFIG_FILE = os.path.join(BASE_DIR, 'config.json')
if not os.path.exists(CONFIG_FILE):
    raise FileNotFoundError(f"Missing config.json at {CONFIG_FILE}")

with open(CONFIG_FILE, 'r') as f:
    config = json.load(f)

# Set variables from config, with defaults
DATA_PERIOD = str(config.get('DATA_PERIOD', '1y')).strip()
CUSTOM_START_DATE = config.get('CUSTOM_START_DATE', '2024-01-01')
STOP_LOSS_PERCENTAGE_RANGE = tuple(config.get('STOP_LOSS_PERCENTAGE_RANGE', [1, 2]))
STOP_LOSS_STEP = float(config.get('STOP_LOSS_STEP', 0.2))
NUM_SIMULATIONS = int(config.get('NUM_SIMULATIONS', 10000))
CONFIDENCE_LEVEL = float(config.get('CONFIDENCE_LEVEL', 0.95))
SPAN_EWMA = int(config.get('SPAN_EWMA', 60))

TODAY = datetime.now()
TODAY_STR = TODAY.strftime('%Y-%m-%d')

# ------------------------------
# DATA PERIOD PARSING (robust)
# ------------------------------
def parse_data_period(period_str: str, today: datetime):
    s = str(period_str).strip().lower()
    # Accept empty or '1y' as default 1 year
    if s == '' or s == '1y':
        return today - timedelta(days=365), '1 year (default)'
    if s == 'ytd':
        return datetime(today.year, 1, 1), 'YTD'
    if s == 'custom':
        return None, 'custom'
    # months like '6m' or '6 months'
    m = re.match(r'^(\d+)\s*(m|mo|month|months)$', s)
    if m:
        months = int(m.group(1))
        days = int(months * 30)
        return today - timedelta(days=days), f'{months} months'
    # years like '1.5y' or '2 years' or '2y'
    y = re.match(r'^(\d+(\.\d+)?)\s*(y|yr|year|years)$', s)
    if y:
        years = float(y.group(1))
        days = int(years * 365)
        return today - timedelta(days=days), f'{years} years'
    # plain numeric (assume years)
    mnum = re.match(r'^(\d+(\.\d+)?)$', s)
    if mnum:
        years = float(mnum.group(1))
        days = int(years * 365)
        return today - timedelta(days=days), f'{years} years (numeric)'
    # fallback 1 year
    return today - timedelta(days=365), 'fallback 1 year'

parsed_start, period_reason = parse_data_period(DATA_PERIOD, TODAY)
if parsed_start is None and DATA_PERIOD.lower() == 'custom':
    try:
        parsed_start = datetime.strptime(CUSTOM_START_DATE, '%Y-%m-%d')
        period_reason = f'custom {CUSTOM_START_DATE}'
    except Exception as e:
        print(f"Invalid CUSTOM_START_DATE '{CUSTOM_START_DATE}', falling back to 1y. Error: {e}")
        parsed_start = TODAY - timedelta(days=365)
        period_reason = 'fallback 1 year (bad custom)'

START_DATE = parsed_start
START_DATE_STR = START_DATE.strftime('%Y-%m-%d')

print(f"DATA_PERIOD requested: '{DATA_PERIOD}' -> using start date {START_DATE_STR} ({period_reason})")

os.makedirs(PRICE_FOLDER, exist_ok=True)

# ------------------------------
# LOAD PORTFOLIO
# ------------------------------
if not os.path.exists(EXCEL_FILE):
    raise FileNotFoundError(f"Missing portfolio file at {EXCEL_FILE}")

df_portfolio = pd.read_excel(EXCEL_FILE, engine='openpyxl')
tickers = df_portfolio['Ticker'].unique()

# ------------------------------
# UPDATE CURRENT PRICE IN PORTFOLIO
# ------------------------------
if 'Current Price' not in df_portfolio.columns:
    df_portfolio['Current Price'] = np.nan  # create column if it doesn't exist

for idx, row in df_portfolio.iterrows():
    ticker = row['Ticker']
    try:
        ticker_info = yf.Ticker(ticker)
        current_price = None
        # first try info (may be None depending on yfinance)
        try:
            current_price = ticker_info.info.get('regularMarketPrice', None)
        except Exception:
            current_price = None
        # fallback using history
        if current_price is None:
            hist = ticker_info.history(period='1d')
            if not hist.empty:
                current_price = hist['Close'].iloc[-1]
        if current_price is not None:
            df_portfolio.at[idx, 'Current Price'] = current_price
            print(f"Updated {ticker} current price: {current_price}")
        else:
            print(f"Could not fetch current price for {ticker}")
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

# Save updated portfolio
df_portfolio.to_excel(EXCEL_FILE, index=False)
print(f"\nPortfolio updated with current prices in {EXCEL_FILE}")

# ------------------------------
# FETCH OR LOAD PRICE DATA
# ------------------------------
all_data = {}

def clean_csv(file_path):
    df = pd.read_csv(file_path)
    df = df.loc[df['Adj Close'].notna()]
    df = df[['Date','Open','High','Low','Close','Adj Close','Volume']]
    df.to_csv(file_path, index=False)

for ticker in tickers:
    file_path = os.path.join(PRICE_FOLDER, f"{ticker}.csv")
    ticker_data = pd.DataFrame()
    
    # Load existing CSV if available
    if os.path.exists(file_path):
        try:
            ticker_data = pd.read_csv(file_path, parse_dates=['Date']).set_index('Date')
            # Force numeric conversion
            for col in ['Open','High','Low','Close','Adj Close','Volume']:
                if col in ticker_data.columns:
                    ticker_data[col] = pd.to_numeric(ticker_data[col], errors='coerce')
            ticker_data = ticker_data.dropna(subset=['Adj Close'])
            ticker_data = ticker_data.sort_index()
            print(f"Loaded {ticker} data from local file. Range: {ticker_data.index.min().date()} -> {ticker_data.index.max().date()}")
        except Exception as e:
            print(f"Warning: Failed to load {ticker} CSV. Will re-download. Error: {e}")
            ticker_data = pd.DataFrame()
    
    # Decide whether download is needed:
    need_download = False
    if ticker_data.empty:
        need_download = True
    else:
        # If local data starts after the requested START_DATE, re-download full requested period
        if ticker_data.index.min() > pd.to_datetime(START_DATE_STR):
            need_download = True
            print(f"{ticker} local data starts at {ticker_data.index.min().date()}, which is after requested start {START_DATE_STR}. Will re-download from {START_DATE_STR}.")
        # If local data doesn't include today's date, append from last date+1
        elif ticker_data.index.max().strftime('%Y-%m-%d') < TODAY_STR:
            need_download = True

    if need_download:
        # Start from either the requested START_DATE or the day after last available
        if ticker_data.empty or ticker_data.index.min() > pd.to_datetime(START_DATE_STR):
            start_date = START_DATE_STR
        else:
            start_date = (ticker_data.index.max() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"Requesting {ticker} data from {start_date} to {TODAY_STR}...")
        new_data = yf.download(ticker, start=start_date, end=TODAY_STR, interval='1d', auto_adjust=False)
        if not new_data.empty:
            new_data.reset_index(inplace=True)
            new_data.to_csv(file_path, index=False)
            clean_csv(file_path)
            ticker_data = pd.read_csv(file_path, parse_dates=['Date']).set_index('Date')
            for col in ['Open','High','Low','Close','Adj Close','Volume']:
                if col in ticker_data.columns:
                    ticker_data[col] = pd.to_numeric(ticker_data[col], errors='coerce')
            ticker_data = ticker_data.dropna(subset=['Adj Close'])
            ticker_data = ticker_data.sort_index()
            print(f"Downloaded/Updated {ticker} data and saved locally. New range: {ticker_data.index.min().date()} -> {ticker_data.index.max().date()}")
        else:
            print(f"Warning: No new data returned for {ticker}. Current local range (if any): {ticker_data.index.min() if not ticker_data.empty else 'none'}")
    
    # Filter ticker_data to the requested START_DATE..TODAY range
    if not ticker_data.empty:
        ticker_data = ticker_data.loc[ticker_data.index >= pd.to_datetime(START_DATE_STR)]
        if ticker_data.empty:
            print(f"After filtering to requested period ({START_DATE_STR} -> {TODAY_STR}), {ticker} has NO data. Skipping.")
            continue
        # Recalculate returns in case rows changed
        ticker_data['Daily Return'] = ticker_data['Adj Close'].pct_change() * 100
    else:
        print(f"Skipping {ticker}: no valid price data.")
        continue
    
    all_data[ticker] = ticker_data

# ------------------------------
# CALCULATE STATISTICS USING EWMA
# ------------------------------
stats_list = []

for ticker in tickers:
    ticker_data = all_data.get(ticker)
    if ticker_data is None or 'Daily Return' not in ticker_data.columns:
        continue
    returns = ticker_data['Daily Return'].dropna() / 100
    if returns.empty:
        print(f"{ticker}: no returns in requested period -> skipping stats.")
        continue
    
    ewma_mu = returns.ewm(span=SPAN_EWMA).mean().iloc[-1]
    ewma_sigma = returns.ewm(span=SPAN_EWMA).std().iloc[-1]
    
    stats_list.append({
        'Ticker': ticker,
        'EWMA Avg Daily Return (%)': ewma_mu*100,
        'EWMA Annualized Volatility (%)': ewma_sigma*np.sqrt(252)*100,
        'Max Daily Return (%)': returns.max()*100,
        'Min Daily Return (%)': returns.min()*100
    })

df_stats = pd.DataFrame(stats_list)
print(f"\nStatistics ({DATA_PERIOD}) using EWMA:")
print(df_stats if not df_stats.empty else "No statistics available for selected period.")

# ------------------------------
# SIMULATE TRAILING STOP LIKELIHOOD USING EWMA
# ------------------------------
def simulate_trailing_stop(ticker_data, stop_loss_pct, num_sim=NUM_SIMULATIONS, span=SPAN_EWMA):
    returns = ticker_data['Daily Return'].dropna() / 100
    if returns.empty:
        return None  # caller will handle None
    
    last_price = ticker_data['Adj Close'].iloc[-1]
    stop_price = last_price * (1 - stop_loss_pct/100)
    
    mu = returns.ewm(span=span).mean().iloc[-1]
    sigma = returns.ewm(span=span).std().iloc[-1]
    
    # if sigma is NaN or zero, handle gracefully
    if pd.isna(sigma) or sigma == 0:
        return 0.0
    
    simulations = np.random.normal(loc=mu, scale=sigma, size=(num_sim, 30))
    price_paths = last_price * np.cumprod(1 + simulations, axis=1)
    hit_stop = np.any(price_paths <= stop_price, axis=1)
    return np.mean(hit_stop)

# ------------------------------
# CALCULATE POTENTIAL LOSS AND VAR USING EWMA
# ------------------------------
results = []

for idx, row in df_portfolio.iterrows():
    ticker = row['Ticker']
    ticker_data = all_data.get(ticker)
    if ticker_data is None or 'Daily Return' not in ticker_data.columns:
        print(f"Skipping {ticker}: no data for risk analysis.")
        continue
    
    returns = ticker_data['Daily Return'].dropna() / 100
    if returns.empty:
        print(f"Skipping {ticker}: no returns in selected period.")
        continue

    last_price = ticker_data['Adj Close'].iloc[-1]
    
    # generate stop %s using numpy arange with float step
    stop_values = np.arange(STOP_LOSS_PERCENTAGE_RANGE[0],
                            STOP_LOSS_PERCENTAGE_RANGE[1] + STOP_LOSS_STEP/2,
                            STOP_LOSS_STEP)
    for stop_pct in stop_values:
        stop_pct = round(float(stop_pct), 2)

        likelihood = simulate_trailing_stop(ticker_data, stop_pct)
        if likelihood is None:
            print(f"{ticker} - stop {stop_pct}%: cannot compute likelihood (no returns). Skipping.")
            continue

        stop_price = last_price * (1 - stop_pct/100)
        potential_loss = (last_price - stop_price) * row.get('Position', 1)
        
        ewma_sigma = returns.ewm(span=SPAN_EWMA).std().iloc[-1]
        ewma_mu = returns.ewm(span=SPAN_EWMA).mean().iloc[-1]
        # Keep VaR as historical percentile; you can change to EWMA-tail later
        var_pct = -np.percentile(returns, (1 - CONFIDENCE_LEVEL)*100)
        var_value = var_pct * last_price * row.get('Position', 1)
        
        results.append({
            'Ticker': ticker,
            'Trailing Stop (%)': stop_pct,
            'Likelihood of Activation (%)': likelihood*100,
            'Potential Loss ($)': potential_loss,
            'EWMA VaR ($)': var_value
        })

df_results = pd.DataFrame(results)
print(f"\nTrailing Stop & Risk Analysis ({DATA_PERIOD}, EWMA):")
print(df_results if not df_results.empty else "No results to display for selected period.")

out_name = f'trailing_stop_analysis_ewma_{DATA_PERIOD.replace(" ", "_")}.xlsx'
df_results.to_excel(out_name, index=False)
print(f"\nResults saved to {out_name}")
