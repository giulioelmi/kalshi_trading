import requests
import os
from dotenv import load_dotenv
from get_data import get_markets_data
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import os, time, uuid, base64, requests

def _load_private_key(pem_text: str):
    return serialization.load_pem_private_key(pem_text.encode("utf-8"), password=None)

def _sign(private_key, timestamp_ms: str, method: str, path: str) -> str:
    path_no_query = path.split("?")[0]
    msg = f"{timestamp_ms}{method.upper()}{path_no_query}".encode("utf-8")
    sig = private_key.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")




def get_bet_info(df):
    best_bet = df.loc[df["edge_no_cents"].idxmax()]
    return best_bet

import os, time, uuid, base64, requests
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

def _load_private_key(pem_text: str):
    return serialization.load_pem_private_key(pem_text.encode("utf-8"), password=None)

def _sign(private_key, timestamp_ms: str, method: str, path: str) -> str:
    path_no_query = path.split("?")[0]
    msg = f"{timestamp_ms}{method.upper()}{path_no_query}".encode("utf-8")
    sig = private_key.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")

def send_order(df):
    load_dotenv()
    private_key_pem = os.getenv("PRIVATE_KEY")  # PEM text: -----BEGIN PRIVATE KEY-----...
    key_id = os.getenv("KEY_ID")                # API key ID (uuid-like)
    if not private_key_pem or not key_id:
        raise ValueError("Missing PRIVATE_KEY or KEY_ID in .env")

    best_bet = get_bet_info(df)
    ticker = best_bet["market_ticker"]

    # Kalshi order prices are integer cents.
    # If best_bet["p_no"] is a probability 0..1, convert to cents:
    no_price = int(round(float(best_bet["p_no"]) * 100)) + 2
    no_price = max(1, min(99, no_price))

    path = "/trade-api/v2/portfolio/orders"
    url = "https://api.elections.kalshi.com" + path

    payload = {
        "ticker": ticker,
        "side": "no",
        "action": "buy",
        "type": "limit",
        "count": 1,
        "no_price": no_price,
        "client_order_id": str(uuid.uuid4()),
    }

    timestamp_ms = str(int(time.time() * 1000))
    private_key = _load_private_key(private_key_pem)
    signature = _sign(private_key, timestamp_ms, "POST", path)

    headers = {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "KALSHI-ACCESS-SIGNATURE": signature,
    }

    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    print(resp.status_code, resp.text)
    resp.raise_for_status()
    return resp.json()


