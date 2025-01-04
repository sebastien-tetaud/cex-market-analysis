import os

import ccxt
import pandas as pd
from dotenv import load_dotenv

from utils.general import OHLCVScraper

load_dotenv()

if __name__ == "__main__":
    
    API_KEY = os.getenv('BITGET_API_KEY')
    SECRET_KEY = os.getenv('BITGET_SECRET_KEY')
    PASSWORD = os.getenv('BITGET_PASSWORD')
    MARKET_TYPE = "future"
    EXCHANGE_ID = "bitget"
    PATH_SAVE = f"/home/ubuntu/project/finance/cex-market-analysis/src/data/{EXCHANGE_ID}/{MARKET_TYPE}"
    TIMEFRAME = '1m'
    SYMBOL = "XRP/USDT:USDT"
    START_DATE_STR = "2024-12-28 00:00:00"
    END_DATE_STR = "2025-01-01 00:00:00"

    exchange = getattr(ccxt, EXCHANGE_ID)({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'password': PASSWORD,
    'options': {
        'defaultType': MARKET_TYPE},
        'enableRateLimit': True
    })
    
    scraper = OHLCVScraper(path_save=PATH_SAVE, exchange=exchange, exchange_id=EXCHANGE_ID)
    scraper.scrape_candles_to_csv(
                    symbol=SYMBOL,
                    timeframe=TIMEFRAME,
                    start_date_str=START_DATE_STR,
                    end_date_str=END_DATE_STR,
                    limit=100)     