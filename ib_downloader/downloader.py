# https://kshitijbanerjee.com/2025/02/01/syncing-historical-data-from-ibkr/

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import pandas as pd
import threading
import time
from datetime import datetime, timedelta
import pytz
import os
from dataclasses import dataclass
from typing import List, Literal
import argparse

@dataclass
class DownloadOptions:
   ticker: str
   start_date: str
   end_date: str
   storage_dir: str
   durationStr: str = '1 D'
   barSizeSetting: str = '1 min'
   
class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = []
        self.data_event = threading.Event()
        self.request_complete = False

    def historicalData(self, reqId, bar):
        try:
            raw_date = bar.date

            # Minute / intraday bars
            if " " in raw_date:
                date_str, tz_str = raw_date.rsplit(" ", 1)
                naive_dt = datetime.strptime(date_str, "%Y%m%d %H:%M:%S")
                tz = pytz.timezone(tz_str)
                localized_dt = tz.localize(naive_dt)
                timestamp = localized_dt.astimezone(pytz.utc)

            # Daily / Weekly bars
            else:
                naive_dt = datetime.strptime(raw_date, "%Y%m%d")
                # Convention: Daily candle at midnight UTC
                timestamp = pytz.utc.localize(naive_dt)

            self.data.append({
                "timestamp": timestamp,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            })

        except Exception as e:
            print(f"Error processing bar ({bar.date}): {e}")

    def historicalDataEnd(self, reqId, start, end):
        self.request_complete = True
        self.data_event.set()

    def run_loop(self):
        self.run()

def download_daily_data(cfg: DownloadOptions):
    """
    Downloads DAILY OHLC data directly from IBKR and stores it as
    one Parquet file per symbol under: <storage_dir>/daily/<TICKER>.parquet
    """

    app = IBapi()
    app.connect('127.0.0.1', 7496, clientId=3)

    # Wait for connection
    start_time = time.time()
    while not app.isConnected():
        if time.time() - start_time > 10:
            raise Exception("Connection timeout")
        time.sleep(0.1)

    api_thread = threading.Thread(target=app.run_loop, daemon=True)
    api_thread.start()
    time.sleep(1)

    # Contract
    contract = Contract()
    contract.symbol = cfg.ticker
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'

    # IB expects duration, not explicit start date
    start_date = datetime.strptime(cfg.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(cfg.end_date, "%Y-%m-%d")
    duration_days = (end_date - start_date).days + 1

    if duration_days <= 365:
        durationStr = f"{duration_days} D"
    else:
        years = (duration_days + 364) // 365  # aufrunden
        durationStr = f"{years} Y"

    end_date_str = end_date.strftime("%Y%m%d 23:59:59 US/Eastern")

    app.reqHistoricalData(
        reqId=99,
        contract=contract,
        endDateTime=end_date_str,
        durationStr=durationStr,
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=1,        # Daily bars usually RTH only
        formatDate=1,
        keepUpToDate=False,
        chartOptions=[]
    )

    if not app.data_event.wait(60):
        raise TimeoutError("Timeout waiting for daily data")

    if not app.data:
        print("No daily data received.")
        return

    # Convert to DataFrame
    df_new = pd.DataFrame(app.data)

    df_new.set_index("timestamp", inplace=True)
    df_new.index = df_new.index.normalize()  # midnight UTC
    df_new = df_new.sort_index()

    start_dt = pd.to_datetime(cfg.start_date).tz_localize("UTC")
    end_dt = pd.to_datetime(cfg.end_date).tz_localize("UTC")
    df_new = df_new.loc[start_dt:end_dt]

    # Storage
    daily_dir = os.path.join(cfg.storage_dir, "daily")
    os.makedirs(daily_dir, exist_ok=True)
    daily_file = os.path.join(daily_dir, f"{cfg.ticker}.parquet")

    # Merge with existing data (incremental update)
    if os.path.exists(daily_file):
        df_existing = pd.read_parquet(daily_file)
        df = pd.concat([df_existing, df_new])
        df = df[~df.index.duplicated(keep="last")]
    else:
        df = df_new

    df = df.sort_index()

    df.to_parquet(
        daily_file,
        compression="snappy",
        index=True
    )

    print(f"Saved DAILY data: {cfg.ticker} ({len(df)} rows)")

    app.disconnect()
    api_thread.join(timeout=2)

def download_historical_data(cfg: DownloadOptions):
    # Initialize API
    app = IBapi()
    app.connect('127.0.0.1', 7496, clientId=2)

    # Connection timeout handling
    connect_timeout = 10
    start_time = time.time()
    while not app.isConnected():
        if time.time() - start_time > connect_timeout:
            raise Exception("Connection timeout")
        time.sleep(0.1)

    # Start API thread
    api_thread = threading.Thread(target=app.run_loop, daemon=True)
    api_thread.start()
    time.sleep(1)  # Stabilization period

    # Contract setup
    contract = Contract()
    contract.symbol = cfg.ticker
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'

    # Date handling
    start_date = datetime.strptime(cfg.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(cfg.end_date, "%Y-%m-%d")

    # Loop through requests
    current_date = start_date
    while current_date <= end_date:
        end_date_str = current_date.strftime("%Y%m%d 23:59:59 US/Eastern")
        next_date = current_date + timedelta(days=1)

        # Request parameters
        app.reqHistoricalData(
            reqId=1,
            contract=contract,
            endDateTime=end_date_str,
            durationStr='1 D',
            barSizeSetting='1 min',
            whatToShow='TRADES',
            useRTH=0,  # Use all available data, not just Regular Trading Hours
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )

        # Wait for data with timeout
        if not app.data_event.wait(30):
            print(f"Timeout waiting for data on {current_date.strftime('%Y-%m-%d')}")
            return

        # Process and save data
        if app.data:
            df = pd.DataFrame(app.data)
            df.set_index('timestamp', inplace=True)

            # Create directory structure
            year = current_date.strftime("%Y")
            month = current_date.strftime("%m")
            filename = f"{current_date.strftime('%Y-%m-%d')}.parquet"
            storage_dir = get_storage_dir(cfg.storage_dir, cfg.ticker, year, month)
            os.makedirs(storage_dir, exist_ok=True)

            # Save with partitioning
            df.to_parquet(
                os.path.join(storage_dir, filename),
                compression='snappy',
                index=True
            )
            print(f"Saved {len(df)} rows for {current_date.strftime('%Y-%m-%d')}")

            # Clear data for next request
            app.data = []
            app.data_event.clear()
        else:
            print("NO DATA!")

        current_date = next_date

    app.disconnect()
    api_thread.join(timeout=2)

def get_storage_dir(path: str, ticker: str, year: str, month: str) -> str:
    return os.path.join(path, ticker, year, month)

def load_parquet_files(directory: str) -> pd.DataFrame:
  """
  Loads and concatenates all Parquet files in the specified directory,
  assuming filenames start with a date in 'YYYY-MM-DD' format.

  Args:
      directory (str): Path to the directory containing Parquet files.

  Returns:
      pd.DataFrame: A concatenated DataFrame of all Parquet files sorted by index.
  """
  data = []
  file_paths: List[tuple[str, str]] = []

  # Walk through directories and collect the file paths with their associated dates
  for root, _, files in os.walk(directory):
      for file in files:
          if file.endswith(".parquet.snappy"):
              file_path = os.path.join(root, file)
              date_str = file.split('_')[0]  # Modify if the date is in a different part of the filename
              file_paths.append((file_path, date_str))

  # Sort file paths by the date
  file_paths.sort(key=lambda x: x[1])

  # Read the files in order of their dates
  for file_path, _ in file_paths:
      print(f"Reading file: {file_path}")
      df = pd.read_parquet(file_path, engine='pyarrow')
      data.append(df)

  # Concatenate all the data into a single DataFrame
  if data:
      full_df = pd.concat(data, ignore_index=False)
      full_df = full_df.sort_index().drop_duplicates()
      print("Data loaded into DataFrame successfully.")
      return full_df
  else:
      print("No Parquet files found in the directory.")
      return pd.DataFrame()

def resample_candles(df: pd.DataFrame, interval: Literal['15min', '1D', '1W']) -> pd.DataFrame:
  """
  Resamples minute-level candlestick data to higher timeframes.
  IMP: The input data is expected to be a minute-level candle

  Args:
      df (pd.DataFrame): The input DataFrame with minute-level data.
      interval (Literal['15min', '1D', '1W']): The resampling interval.

  Returns:
      pd.DataFrame: The resampled DataFrame.
  """
  resampled_df = df.resample(interval).agg({
      'open': 'first',
      'high': 'max',
      'low': 'min',
      'close': 'last',
      'volume': 'sum'
  }).dropna()
  return resampled_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IB Data Downloader CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Download command
    download_parser = subparsers.add_parser("download", help="Download historical data from Interactive Brokers")
    download_parser.add_argument("--ticker", required=True, help="Ticker symbol (e.g., TSLA)")
    download_parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    download_parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    download_parser.add_argument("--storage-dir", required=True, help="Base directory for storage")

    # Resample command
    resample_parser = subparsers.add_parser("resample", help="Resample candlestick data")
    resample_parser.add_argument("file", type=str, help="Path to the Parquet file containing minute-level data.")
    resample_parser.add_argument("interval", type=str, choices=['15min', '1D', '1W'], help="Resampling interval.")

    args = parser.parse_args()

    if args.command == "download":
        cfg = DownloadOptions(
            ticker=args.ticker,
            start_date=args.start_date,
            end_date=args.end_date,
            storage_dir=args.storage_dir
        )
        download_historical_data(cfg)
    elif args.command == "resample":
        df = pd.read_parquet(args.file)
        df.index = pd.to_datetime(df.index)  # Ensure index is datetime
        resampled_df = resample_candles(df, args.interval)
        print(resampled_df.head())
    else:
        parser.print_help()