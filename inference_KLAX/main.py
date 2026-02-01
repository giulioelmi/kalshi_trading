from get_data import get_text, normalize_cli_text, extract_cli_yesterday, extract_cli_today, get_forecast, get_markets_data, merge_data, save_results
from model import feature_engineering, make_prediction, get_ev
from create_orders import get_bet_info, send_order

best_model = "/Users/giulioelmi/Desktop/kalshi_trading/inference_KLAX/best1_1.json"
ticker = "KXHIGHLAX"
office = "LOX"
grid = "149,41"
sigma = 2.5324872296670837
data_file_path = "/Users/giulioelmi/Desktop/kelshi_trading/inference_KLAX/prediction_log.csv"

def main():
    merged_data = merge_data(extract_cli_yesterday(), extract_cli_today(), get_forecast(office, grid))

    model_data = feature_engineering(merged_data)
    print(model_data)

    adjusted_forecast = make_prediction(model_data, best_model)

    market_data = get_markets_data(ticker)
    
    ev_df = get_ev(market_data, adjusted_forecast, sigma=sigma)

    #save_results(data_file_path, merged_data, adjusted_forecast)
    
    print(ev_df[["market_ticker", "floor", "cap", "edge_yes_cents", "edge_no_cents", "p_yes", "p_no"]])
    
    send_order(ev_df)


if __name__ == "__main__":
    main()
