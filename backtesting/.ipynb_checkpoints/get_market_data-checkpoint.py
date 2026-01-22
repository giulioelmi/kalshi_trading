import requests
import re
import pandas as pd
from datetime import datetime, date, time, timedelta, timezone

market_ticker = "KXHIGHLAX"

def get_event_candles(series_ticker, event_ticker, start_ts, end_ts, target_ts=None):
    """
    Get candles for an event. If target_ts is provided, returns the candle
    closest to but not after the target timestamp.
    """
    url = f"https://api.elections.kalshi.com/trade-api/v2/series/{series_ticker}/events/{event_ticker}/candlesticks"
    params = {"start_ts": int(start_ts), "end_ts": int(end_ts), "period_interval": 1}

    r = requests.get(url, params=params)
    r.raise_for_status()

    response = r.json()

    rows = []

    for ticker, candles in zip(response["market_tickers"], response["market_candlesticks"]):
        # Skip if no candles in this time window
        if not candles:
            continue
            
        # parse threshold from market ticker (…-T62 or …-B66.5)
        m = re.search(r"-([TB])(\d+(?:\.\d+)?)$", ticker)
            
        threshold = float(m.group(2))
        threshold_type = m.group(1)

        # If target_ts is specified, find the candle closest to but not after target_ts
        if target_ts is not None:
            # Filter candles that end at or before target time
            valid_candles = [c for c in candles if c["end_period_ts"] <= target_ts]
            if valid_candles:
                # Get the candle with the latest end_period_ts that's <= target_ts
                c = max(valid_candles, key=lambda x: x["end_period_ts"])
            else:
                # If no candle ends before target, use the earliest one (shouldn't happen with proper window)
                c = min(candles, key=lambda x: x["end_period_ts"])
        else:
            # keep the latest candle (max end_period_ts)
            c = max(candles, key=lambda x: x["end_period_ts"])

        rows.append({
            "ticker": ticker,
            "threshold_type": threshold_type,          
            "threshold": threshold,                    
            "yes_ask": float(c["yes_ask"]["close_dollars"])
        })
    df = pd.DataFrame(rows)
    return df


def _to_date(x):
    # accepts date or "YYYY-MM-DD"
    return x if isinstance(x, date) and not isinstance(x, datetime) else datetime.fromisoformat(str(x)).date()

def _event_ticker_for_day(series_ticker: str, d: date) -> str:
    # e.g., 2026-01-02 -> "26JAN02" -> "KXHIGHLAX-26JAN02"
    suffix = d.strftime("%y%b%d").upper()
    return f"{series_ticker}-{suffix}"

def make_daily_request_table(series_ticker: str, start_date, end_date, target_hhmm="12:00", tz=timezone.utc):
    """
    Returns a DataFrame with one row per day:
    [series_ticker, event_ticker, start_time_iso, end_time_iso, target_time_iso, start_ts, end_ts, target_ts]
    """
    start_d = _to_date(start_date)
    end_d = _to_date(end_date)

    hh, mm = map(int, target_hhmm.split(":"))

    rows = []
    d = start_d
    while d <= end_d:
        event_ticker = _event_ticker_for_day(series_ticker, d)

        target_dt = datetime(d.year, d.month, d.day, hh, mm, tzinfo=tz)

        # widen the interval so you reliably capture >= 1 candle
        start_dt = target_dt - timedelta(minutes=90)
        end_dt   = target_dt + timedelta(minutes=90)

        rows.append({
            "series_ticker": series_ticker,
            "event_ticker": event_ticker,
            "start_time": start_dt.isoformat(),
            "end_time": end_dt.isoformat(),
            "target_time": target_dt.isoformat(),
            "start_ts": int(start_dt.timestamp()),
            "end_ts": int(end_dt.timestamp()),
            "target_ts": int(target_dt.timestamp()),
            "day": d.isoformat(),
        })
        d += timedelta(days=1)

    return pd.DataFrame(rows)

def fetch_daily_candles_from_table(request_table: pd.DataFrame):
    """
    Calls get_event_candles once per row and returns one combined DataFrame
    containing all markets for all days (one candle/day/market, using the last candle <= target_ts).
    """
    out = []
    for _, r in request_table.iterrows():
        df_day = get_event_candles(
            r["series_ticker"],
            r["event_ticker"],
            r["start_ts"],
            r["end_ts"],
            target_ts=r["target_ts"],
        )
        df_day["day"] = r["day"]
        df_day["event_ticker"] = r["event_ticker"]
        out.append(df_day)

    return pd.concat(out, ignore_index=True)

