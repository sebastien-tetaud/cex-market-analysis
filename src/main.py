import pandas as pd
from utils.general import OHLCVScraper

if __name__ == "__main__":
    

    exchange_id = "bitget"
    path_save = f"/home/ubuntu/project/finance/cex-market-analysis/src/data/test"
    timeframe = '1m'
    symbol = "BTC/USDT"
    scraper = OHLCVScraper(path_save=path_save, exchange_id=exchange_id)
    scraper.scrape_candles_to_csv(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_date_str="2024-12-15T00:00:00",
                    end_date_str="2025-01-01T00:00:00",
                    limit=100)      
