import pandas as pd
import math
import numpy as np

from xgboost import XGBRegressor
import xgboost as xgb

def feature_engineering(df):
    """
    Performs feature engineering on the merged DataFrame to match model features.
    """
    print("starting feature engineering")
    df = df
    df = df.sort_values("DATE").reset_index(drop=True)
    cols_to_ffill = ["DATE", "TMAX", "TMIN", "PRCP", "AWND", "WSF2", "WDF2", "forecasted_TMAX"]
    df[cols_to_ffill] = df[cols_to_ffill].ffill()

    df["doy"] = df["DATE"].dt.dayofyear
    df["dow"] = df["DATE"].dt.dayofweek
    df["month"] = df["DATE"].dt.month
    df["doy_sin"] = np.sin(2*np.pi*df["doy"]/365.25)
    df["doy_cos"] = np.cos(2*np.pi*df["doy"]/365.25)
    
    df["diurnal_range"] = df["TMAX"] - df["TMIN"]
    df["wind_dir_sin"] = np.sin(np.deg2rad(df["WDF2"]))
    df["wind_dir_cos"] = np.cos(np.deg2rad(df["WDF2"]))
    
    #lags
    lag_cols_1 = ["TMAX", "TMIN", "diurnal_range"]
    for c in lag_cols_1:
        for k in [1, 2, 3, 7]:
            df[f"{c}_lag{k}"] = df[c].shift(k)
    lag_cols_2 = ["PRCP"]
    for k in [1, 2, 3, 4]:
        df[f"PRCP_lag_{k}"] = df["PRCP"].shift(k)

    #rolling means 
    df["rolling_3"] = df["TMAX"].rolling(3).mean()
    df["rolling_7"] = df["TMAX"].rolling(7).mean()
    df = df.sort_values("DATE", ascending = False).reset_index(drop=True)
    df = df.iloc[:1]
    df = df.drop(columns = ["DATE"])
    return df

def make_prediction(df, best_model_path):
    """
    Makes a prediction using the trained model and adjusts the forecast.
    """
    inference_df = df
    booster = xgb.Booster()
    booster.load_model(best_model_path)
    pred_error = booster.predict(xgb.DMatrix(inference_df))[0]

    forecast = df["forecasted_TMAX"].iloc[0]
    adjusted_forecast = forecast  + pred_error

    print("------------------")
    print("Predicted error (TMAX_obs - TMAX_forecast):", pred_error)
    print("Forecast: ", forecast)
    print("Adjusted forecast: ", adjusted_forecast)

    return adjusted_forecast




def get_ev(markets: pd.DataFrame, mu: float, sigma: float) -> pd.DataFrame:
    """
    For each Kalshi contract row, compute:
      - p_yes: model-implied probability that YES settles to 1
      - max_yes / max_no: maximum price (in cents) you're willing to pay for YES/NO
      - edges vs current asks (optional but useful)

    Assumptions about contract encoding:
      - floor & cap present  -> bucket: floor <= T < cap
      - floor present only   -> unilateral: T >= floor
      - cap present only     -> unilateral: T < cap

    Prices in the input are assumed to be in cents (0..100).
    """

    if sigma <= 0:
        raise ValueError("sigma must be > 0")

    def norm_cdf(x: float) -> float:
        # Standard normal CDF using erf (no scipy dependency)
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    def prob_yes(row) -> float:
        floor = row.get("floor")
        cap = row.get("cap")

        has_floor = pd.notna(floor)
        has_cap = pd.notna(cap)

        if has_floor and has_cap:
            # P(floor <= T < cap)
            z_u = (cap - mu) / sigma
            z_l = (floor - mu) / sigma
            p = norm_cdf(z_u) - norm_cdf(z_l)
        elif has_floor and not has_cap:
            # P(T >= floor)
            z = (floor - mu) / sigma
            p = 1.0 - norm_cdf(z)
        elif has_cap and not has_floor:
            # P(T < cap)
            z = (cap - mu) / sigma
            p = norm_cdf(z)
        else:
            # If both missing, cannot interpret the contract
            p = float("nan")

        # Numerical safety
        if pd.notna(p):
            p = max(0.0, min(1.0, p))
        return p

    out = markets.copy()

    out["p_yes"] = out.apply(prob_yes, axis=1)
    out["p_no"] = 1.0 - out["p_yes"]

    # Maximum you're willing to pay (in cents)
    out["max_yes_cents"] = (100.0 * out["p_yes"]).round(2)
    out["max_no_cents"] = (100.0 * out["p_no"]).round(2)

    # Compare to current asks (also in cents)
    if "yes_ask" in out.columns:
        out["edge_yes_cents"] = (out["max_yes_cents"] - out["yes_ask"]).round(2)
    if "no_ask" in out.columns:
        out["edge_no_cents"] = (out["max_no_cents"] - out["no_ask"]).round(2)

    # Handy boolean suggestions (strictly positive edge)
    if "yes_ask" in out.columns:
        out["buy_yes"] = out["edge_yes_cents"] > 0
    if "no_ask" in out.columns:
        out["buy_no"] = out["edge_no_cents"] > 0

    return out