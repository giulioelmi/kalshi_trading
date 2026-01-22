import requests
import pandas as pd
import re
import datetime as dt
from datetime import datetime
from io import StringIO


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
        "TMAX": r"TODAY\s+MAXIMUM\s+(\d+)",
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


def save_results(path, wether_data, adjusted_forecast):
    data = wether_data.iloc[[0]]
    data["correction"] = correction
    data["adjusted_forecast"] = adjusted_forecast
    data.to_csv(path, mode = "a", header = False, index = False)


def fetch_nextday_tmax_lax(start_date, end_date, asof_hour_utc=12):
    API_TOKEN = "c60a377573a67faffeb88889da834a48508c8110"
    LAT_LAX = 33.942
    LON_LAX = -118.408
    BASE_URL = "https://gribstream.com/api/v2/nbm/history"
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/csv",
        "Authorization": f"Bearer {API_TOKEN}",
    }

    rows = []
    curr = start_date
    total_days = (end_date - start_date).days + 1
    day_index = 1

    while curr <= end_date:
        # overwrite the same line every iteration
        print(f"\rFetching day {day_index}/{total_days} ({curr}) ...", end="", flush=True)

        from_dt = dt.datetime(curr.year, curr.month, curr.day, 0, 0, tzinfo=dt.timezone.utc)
        until_dt = from_dt + dt.timedelta(days=1)

        prev_day = from_dt - dt.timedelta(days=1)
        asof_dt = prev_day.replace(hour=asof_hour_utc, minute=0, second=0, microsecond=0)

        payload = {
            "fromTime": from_dt.isoformat().replace("+00:00", "Z"),
            "untilTime": until_dt.isoformat().replace("+00:00", "Z"),
            "asOf": asof_dt.isoformat().replace("+00:00", "Z"),
            "coordinates": [{"lat": LAT_LAX, "lon": LON_LAX, "name": "KLAX"}],
            "variables": [{"name": "TMP", "level": "2 m above ground", "info": "", "alias": "tempK"}],
        }

        try:
            resp = requests.post(BASE_URL, json=payload, headers=headers, timeout=15)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text))
        except Exception:
            curr += dt.timedelta(days=1)
            day_index += 1
            continue

        if not df.empty:
            df["forecasted_time"] = pd.to_datetime(df["forecasted_time"])
            df["tempC"] = df["tempK"] - 273.15
            df["tempF"] = df["tempC"] * 9/5 + 32

            rows.append({
                "date_utc": curr.isoformat(),
                "asof_utc": asof_dt.isoformat().replace("+00:00", "Z"),
                "tmax_K": df["tempK"].max(),
                "tmax_C": df["tempC"].max(),
                "tmax_F": df["tempF"].max(),
            })

        curr += dt.timedelta(days=1)
        day_index += 1

    # after finishing, print a clean newline
    print()
    return pd.DataFrame(rows)