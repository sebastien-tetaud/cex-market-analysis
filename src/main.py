from utils.general import OHLCVScraper
import pandas as pd



if __name__ == "__main__":
    
    
    scraper = OHLCVScraper(exchange_id="bitget")
    df = pd.read_csv("/home/ubuntu/project/finance/cex-market-analysis/symbols/bitget.csv")
    symbols = df["symbol"].values
    timeframes = ['1d', '12h', '6h', '4h', '2h', '1h', '30m', '15m', '5m']
    for symbol in symbols:
        for timeframe in timeframes:

            scraper.scrape_candles_to_csv(
                symbol=symbol,
                timeframe=timeframe,
                start_date_str="2019-01-01T00:00:00",
                end_date_str="2025-01-01T00:00:00",
                limit=100
                )       
