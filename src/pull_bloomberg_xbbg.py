import pandas as pd
from xbbg import blp
import settings
from pathlib import Path

# Load constants from settings.py
DATA_DIR = settings.DATA_DIR
START_DATE = settings.START_DATE
END_DATE = settings.END_DATE

# Define tickers for Spot Indices
spot_tickers = ["SPX Index", "NDX Index", "INDU Index"]
spot_fields = ["PX_LAST", "IDX_EST_DVD_YLD", "INDX_GROSS_DAILY_DIV"]

# Define tickers for Futures
futures_tickers = {
    "S&P 500": ["ES1 Index", "ES2 Index", "ES3 Index", "ES4 Index"],
    "Nasdaq": ["NQ1 Index", "NQ2 Index", "NQ3 Index", "NQ4 Index"],
    "Dow Jones": ["DM1 Index", "DM2 Index", "DM3 Index", "DM4 Index"]
}
futures_fields = ["PX_LAST", "PX_VOLUME", "OPEN_INT", "CURRENT_CONTRACT_MONTH_YR"]

# Define OIS tickers
ois_tickers = [
    "USSO1Z CMPN Curncy", "USSOA CMPN Curncy", "USSOB CMPN Curncy", "USSOC CMPN Curncy",
    "USSOF CMPN Curncy", "USSO1 CMPN Curncy", "USSO2 CMPN Curncy", "USSO3 CMPN Curncy",
    "USSO4 CMPN Curncy", "USSO5 CMPN Curncy", "USSO7 CMPN Curncy", "USSO10 CMPN Curncy",
    "USSO15 CMPN Curncy", "USSO20 CMPN Curncy", "USSO30 CMPN Curncy"
]

def fetch_spot_indices():
    """Fetches historical Bloomberg data for spot indices (SPX, NDX, INDU)."""
    print("Fetching historical data for spot indices...")
    return blp.bdh(spot_tickers, flds=spot_fields, start_date=START_DATE, end_date=END_DATE)

def fetch_futures_data():
    """Fetches historical Bloomberg data for futures contracts."""
    futures_data = {}
    for category, tickers in futures_tickers.items():
        print(f"Fetching historical futures data for {category}...")
        try:
            data = blp.bdh(tickers, flds=futures_fields, start_date=START_DATE, end_date=END_DATE)
            futures_data[category] = data
        except Exception as e:
            print(f"Error fetching futures data for {category}: {e}")
    return futures_data

def fetch_ois_data():
    """Fetches historical Bloomberg data for OIS rates."""
    print("Fetching historical OIS rate data...")
    return blp.bdh(ois_tickers, flds="PX_LAST", start_date=START_DATE, end_date=END_DATE)

if __name__ == "__main__":
    # Fetch historical data
    spot_indices_data = fetch_spot_indices()
    futures_data = fetch_futures_data()
    ois_data = fetch_ois_data()

    # Combine all data into a single DataFrame
    all_data = pd.concat([spot_indices_data] + list(futures_data.values()) + [ois_data], axis=1)

    # Save the results
    path = Path(DATA_DIR) / "bloomberg_historical_data.parquet"
    all_data.to_parquet(path)

    print(f"Historical data saved to {path}")
