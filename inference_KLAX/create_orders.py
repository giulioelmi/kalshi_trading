import requests

def send_order(ticker, side, count = 1, yes_price = 0, no_price = 0, )
url = "https://api.elections.kalshi.com/trade-api/v2/portfolio/orders"

payload = {
    "ticker": "<string>",
    "side": "yes",
    "action": "buy",
    "count": 2,
    "client_order_id": "<string>",
    "type": "limit",
    "yes_price": 50,
    "no_price": 50,
    "yes_price_dollars": "0.5600",
    "no_price_dollars": "0.5600",
    "expiration_ts": 123,
    "time_in_force": "fill_or_kill",
    "buy_max_cost": 123,
    "post_only": True,
    "reduce_only": True,
    "sell_position_floor": 123,
    "self_trade_prevention_type": "taker_at_cross",
    "order_group_id": "<string>",
    "cancel_order_on_pause": True
}
headers = {
    "KALSHI-ACCESS-KEY": "<api-key>",
    "KALSHI-ACCESS-SIGNATURE": "<api-key>",
    "KALSHI-ACCESS-TIMESTAMP": "<api-key>",
    "Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)

print(response.text)