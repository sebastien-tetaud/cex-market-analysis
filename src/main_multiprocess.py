from utils.general import OHLCVScraper
import pandas as pd
from utils.general import get_top_usdt_symbol_by_volume

from multiprocessing import Pool

def scrape_data(args):
    """
    Function to scrape data for a given symbol and timeframe.
    Args:
        args (tuple): A tuple containing symbol, timeframe, and scraper instance.
    """
    symbol, timeframe, scraper = args
    try:
        scraper.scrape_candles_to_csv(
            symbol=symbol,
            timeframe=timeframe,
            start_date_str="2017-08-01T00:00:00",
            end_date_str="2025-01-01T00:00:00",
            limit=100
        )
    except Exception as e:
        print(f"Error scraping {symbol} {timeframe}: {e}")

if __name__ == "__main__":
    
    # Example usage
    
    exchange_id = "binance"
    path_save = f"/home/ubuntu/project/finance/cex-market-analysis/src/data/{exchange_id}/spot"
    timeframes = ['1d', '12h', '6h', '4h', '1h', '30m', '15m', '5m']

    top_symbols = get_top_usdt_symbol_by_volume(exchange_id, top_n=100)
    symbols = top_symbols["symbol"].values
    scraper = OHLCVScraper(path_save=path_save, exchange_id=exchange_id)
    # Create a list of tasks
    tasks = [(symbol, timeframe, scraper) for symbol in symbols for timeframe in timeframes]
    # Use multiprocessing Pool with 8 processes
    with Pool(processes=8) as pool:
        pool.map(scrape_data, tasks)
