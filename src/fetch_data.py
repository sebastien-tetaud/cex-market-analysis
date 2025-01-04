import os

import ccxt
import pandas as pd
from dotenv import load_dotenv
from utils.general import OHLCVScraper, get_top_symbol_by_volume

load_dotenv()

if __name__ == "__main__":
    
    API_KEY = os.getenv('BITGET_API_KEY')
    SECRET_KEY = os.getenv('BITGET_SECRET_KEY')
    PASSWORD = os.getenv('BITGET_PASSWORD')
    MARKET_TYPE = "future"
    EXCHANGE_ID = "binance"
    PATH_SAVE = f"/home/ubuntu/project/finance/cex-market-analysis/src/data/{EXCHANGE_ID}/{MARKET_TYPE}"
    TIMEFRAME = '1m'
    # SYMBOL = "XRP/USDT:USDT"
    START_DATE_STR = "2024-01-01 00:00:00"
    END_DATE_STR = "2025-01-01 00:00:00"

    exchange = getattr(ccxt, EXCHANGE_ID)({
    # 'apiKey': API_KEY,
    # 'secret': SECRET_KEY,
    # 'password': PASSWORD,
    'options': {
        'defaultType': MARKET_TYPE},
        'enableRateLimit': True
    })
    
    df_symbols = get_top_symbol_by_volume(exchange=exchange,
                                          pair_filter="/USDT:USDT",
                                          top_n=100)
    
    symbols = df_symbols["symbol"].values
    # symbols = ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'XRP/USDT:USDT', 'SOL/USDT:USDT',
    #    'DOGE/USDT:USDT', 'MOCA/USDT:USDT', 'ADA/USDT:USDT',
    #    'PEPE/USDT:USDT', 'SUI/USDT:USDT', 'BIO/USDT:USDT',
    #    'XLM/USDT:USDT', 'UNI/USDT:USDT', 'HBAR/USDT:USDT',
    #    'FARTCOIN/USDT:USDT', 'BGB/USDT:USDT', 'BRETT/USDT:USDT',
    #    'AGLD/USDT:USDT', 'CHILLGUY/USDT:USDT', 'LINK/USDT:USDT',
    #    'ENA/USDT:USDT']
    
    for symbol in symbols:

        try:
            scraper = OHLCVScraper(path_save=PATH_SAVE, exchange=exchange, exchange_id=EXCHANGE_ID)
            scraper.scrape_candles_to_csv(
                            symbol=symbol,
                            timeframe=TIMEFRAME,
                            start_date_str=START_DATE_STR,
                            end_date_str=END_DATE_STR,
                            limit=100)
        
        except:
            print("error")


    # files_path = natsort.natsorted(glob.glob(os.path.join(PATH_SAVE, "*.csv"), recursive=False))
    # for file in files_path:
    #     df = pd.read_csv(file)
    #     df['date'] = pd.to_datetime(df['date'])
    #     df.set_index('date', inplace=True)
    #     missing = check_missing_timestamps(df, freq='1min')
    #     if not missing.empty:
    #         print("Missing timestamps:")
    #         print(file)