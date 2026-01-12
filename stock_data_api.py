#       stock_data_api.py
# This module provides functions to fetch stock price data using yfinance.
#   It includes functions to get the latest price for a specific day
#   and to get the daily closing price on or before a given date.
#   These functions handle date conversions, time zones, and data retrieval

import datetime
import yfinance as yf
from zoneinfo import ZoneInfo
import warnings

def get_latest_price_for_day(ticker, day, interval="1m", include_prepost=False, max_back_days=10):
    eastern = ZoneInfo("America/New_York")
    utc = ZoneInfo("UTC")

    if isinstance(day, datetime.datetime):
        day_date = day.date()
    elif isinstance(day, datetime.date):
        day_date = day
    else:
        day_date = datetime.datetime.strptime(str(day), "%Y-%m-%d").date()

    used_day = day_date
    for _ in range(max_back_days + 1):
        start_et = datetime.datetime(used_day.year, used_day.month, used_day.day, 0, 0, tzinfo=eastern)
        end_et = start_et + datetime.timedelta(days=1)
        start_utc = start_et.astimezone(utc)
        end_utc = end_et.astimezone(utc)

        try:
            df = yf.download(
                ticker,
                start=start_utc,
                end=end_utc,
                interval=interval,
                prepost=include_prepost,
                progress=False,
            )
        except Exception as e:
            print(f"Error downloading data for {ticker} on {used_day}: {e}")
            break
        if df is not None and not df.empty:
            last_time = df.index[-1]
            try:
                last_time_et = last_time.tz_convert(eastern)
            except Exception:
                last_time_et = last_time
            if "Close" in df.columns:
                _val = df["Close"].iloc[-1]
            elif "Adj Close" in df.columns:
                _val = df["Adj Close"].iloc[-1]
            else:
                _num_cols = df.select_dtypes(include="number").columns
                _val = df[_num_cols[0]].iloc[-1] if len(_num_cols) else df.iloc[-1].iloc[0]
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=".*Calling float on a single element Series is deprecated.*",
                    category=FutureWarning,
                )
                latest_price = float(_val)
            return latest_price, last_time_et, used_day

        used_day = used_day - datetime.timedelta(days=1)

    return None, None, used_day
    

def get_daily_close_on_or_before(ticker, day, lookback_days=10, auto_adjust=True):
    """Return (price, date_used) where date_used <= day (prev trading day if needed).

    Uses daily candles via yf.download with a small window around the target date.
    """
    if isinstance(day, datetime.datetime):
        target = day.date()
    elif isinstance(day, datetime.date):
        target = day
    else:
        target = datetime.datetime.strptime(str(day), "%Y-%m-%d").date()

    start = target - datetime.timedelta(days=lookback_days)
    end = target + datetime.timedelta(days=1)  # end is exclusive

    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=auto_adjust,
            progress=False,
        )
    except Exception as e:
        print(f"Error downloading daily data for {ticker} around {target}: {e}")
        return None, None

    if df is None or df.empty:
        return None, None

    # Keep rows up to the target date (handles weekends/holidays)
    idx_dates = df.index.date
    mask = idx_dates <= target
    df2 = df[mask]
    if df2.empty:
        return None, None

    last_row = df2.iloc[-1]
    if "Close" in df2.columns:
        _val = df2["Close"].iloc[-1]
    elif "Adj Close" in df2.columns:
        _val = df2["Adj Close"].iloc[-1]
    else:
        _num_cols = df2.select_dtypes(include="number").columns
        _val = df2[_num_cols[0]].iloc[-1] if len(_num_cols) else df2.iloc[-1].iloc[0]
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*Calling float on a single element Series is deprecated.*",
            category=FutureWarning,
        )
        price = float(_val)
    used_date = df2.index[-1].date()
    return price, used_date
