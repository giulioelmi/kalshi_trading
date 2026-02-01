import requests
import pandas as pd
import re
import datetime as dt
from datetime import datetime


def get_text(v):
    """
    Fetches the CLI report text for a given version number.
    """
    URL = f"https://forecast.weather.gov/product.php?site=LOX&issuedby=LAX&product=CLI&format=TXT&version={v}&glossary=0"
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    return r.text

def normalize_cli_text(text: str) -> str:
    """
    Normalizes CLI report text by standardizing line endings, spaces, and blank lines.
    """
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip()

def extract_cli_yesterday(version = 50):
    """
    Extracts yesterday's weather data from CLI reports up to the specified version number.
    """
    rows = []  
    # patterns to extract data
    patterns = {
        "DATE": r"CA CLIMATE SUMMARY FOR (\w+ \d{1,2} \d{4})",
        "TMAX": r"YESTERDAY\s+MAXIMUM\s+(\d+)",
        "TMIN": r"MINIMUM\s+(\d+)",
        "PRCP": r"PRECIPITATION\s*\(IN\)\s*YESTERDAY\s+([0-9]+(?:\.[0-9]+)?)",
        "AWND": r"AVERAGE WIND SPEED\s+([\d.]+)",
        "WDF2": r"HIGHEST WIND DIRECTION\s+\w+\s+\((\d+)\)",
        "WSF2": r"HIGHEST WIND SPEED\s+(\d+)"
    }
    # iterate through versions to collect data, checking if it contains yesterday's report
    for n in range(1, version):
        text = normalize_cli_text(get_text(n))

        # Match object or None
        valid_yesterday = re.search(r"YESTERDAY", text)
        if not valid_yesterday:
            print(f"no report available for version {n}")
            continue
        print("downloading yesterday's report")

        out = {}
        for k, p in patterns.items():
            m = re.search(p, text, flags=re.MULTILINE | re.DOTALL)
            out[k] = m.group(1) if m else None
        rows.append(out)
    # create DataFrame from collected rows and clean data types
    df = pd.DataFrame(rows)
    df["DATE"] = pd.to_datetime(df["DATE"], format = "mixed")
    for c in ["TMAX", "TMIN", "WSF2", "WDF2", "PRCP", "AWND"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.drop_duplicates(subset = ["DATE"], keep = "first", inplace = True)
    return df


def extract_cli_today(version = 1):
    """
    Extracts today's weather data from the CLI report for the specified version number. Same structure as extract_cli_yesterday but for today's data.
    """
    print("downloading today's report")
    patterns = {
        "DATE": r"CA CLIMATE SUMMARY FOR (\w+ \d{1,2} \d{4})",
        "TMAX": r"TODAY\s+MAXIMUM\s+(\d+)", #change to TODAY
        "TMIN": r"MINIMUM\s+(\d+)",
        "PRCP": r"PRECIPITATION\s*\(IN\)\s*TODAY\s+([0-9]+(?:\.[0-9]+)?)",
        "AWND": r"AVERAGE WIND SPEED\s+([\d.]+)",
        "WDF2": r"HIGHEST WIND DIRECTION\s+\w+\s+\((\d+)\)",
        "WSF2": r"HIGHEST WIND SPEED\s+(\d+)"
        ,
    }
    text = normalize_cli_text(get_text(version))
    valid_today = re.search(r"TEMPERATURE\s*\(F\)[\s\S]*?\bTODAY\b", text)
    if not valid_today:
        raise Exception("No report available for today")  
    out = {}
    for k, p in patterns.items():
        m = re.search(p, text, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        out[k] = m.group(1) if m else None
    df = pd.DataFrame([out])  # one row
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    for c in ["TMAX", "TMIN", "WSF2", "WDF2", "PRCP", "AWND"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def get_forecast(office, grid):
    """
    Fetches the forecasted maximum temperature for tomorrow from the weather.gov API by scraping the hourly forecast data.
    """
    print("Fetching forecast")
    LAT, LON = 33.94, -118.401
    HEADERS = {"User-Agent": "giulio"}
    URL = f"https://api.weather.gov/gridpoints/{office}/{grid}/forecast/hourly"
    tomorrow = (dt.date.today() + dt.timedelta(days=1)).isoformat() 
    today = dt.date.today().isoformat()
    r = requests.get(URL, headers=HEADERS)
    periods = r.json()["properties"]["periods"]
    tmax = -999
    
    for p in periods:
        if p["startTime"][:10] == tomorrow:
            tmax = max(tmax, p["temperature"])
    if tmax == -999:
        print("Cannot fetch forecast")
    else: print("forecast downloaded")
    max_temp_today = {"DATE": today, "forecasted_TMAX": tmax}
    df = pd.DataFrame([max_temp_today])
    df["DATE"] = pd.to_datetime(df["DATE"], format = "mixed")
    df["forecasted_TMAX"] = pd.to_numeric(df["forecasted_TMAX"], errors = "coerce")
    return df

def merge_data(yesterday, today, forecast):
    """merges yesterday's and today's CLI data with the forecasted maximum temperature for tomorrow."""
    df_yesterday = yesterday
    df_today = today
    df_forecast = forecast
    df_merged = pd.concat([df_yesterday, df_today], axis=0)
    df_merged["year"] = df_merged["DATE"].dt.year
    df_inference = pd.merge(df_merged, df_forecast, how = "left", on = "DATE")
    df_inference = df_inference.sort_values("DATE", ascending=False)
    latest = df_inference.iloc[0]
    # check that forecasted_TMAX is not missing for latest date
    if pd.isna(latest.get("forecasted_TMAX")):
        raise RuntimeError(
            "merge_data(): forecasted_TMAX missing for latest DATE "
            f"({latest['DATE'].date()}). Forecast not published yet."
        )
    return df_inference

def get_markets_data(ticker):
    markets_url = f"https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker={ticker}&status=open"
    markets_response = requests.get(markets_url)
    markets_data = markets_response.json()
    rows = []
    for market in markets_data['markets']:
        event_ticker = market.get("event_ticker")
        dt = event_ticker.split("-")[1]
        date = datetime.strptime("20" + dt, "%Y%b%d").date()
        market_ticker = market.get("ticker")
        floor = market.get("floor_strike")
        cap = market.get("cap_strike")
        no_ask = market.get("no_ask")
        yes_ask = market.get("yes_ask")
        data = {"date": date, "event_ticker": event_ticker, "market_ticker": market_ticker, "floor": floor, "cap": cap, "no_ask": no_ask, "yes_ask": yes_ask}
        rows.append(data)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    for c in ["floor", "cap", "no_ask", "yes_ask"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    #print(json.dumps(markets_data, indent=4))
    tomorrow = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
    df = df[df["date"] == tomorrow]
    df = df.sort_values(by = "cap")
    return df

def save_results(path, wether_data, adjusted_forecast):
    data = wether_data.iloc[[0]]
    data["correction"] = correction
    data["adjusted_forecast"] = adjusted_forecast
    data.to_csv(path, mode = "a", header = False, index = False)

print(get_markets_data("KXHIGHLAX"))