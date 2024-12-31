from utils.general import OHLCVScraper

if __name__ == "__main__":
    scraper = OHLCVScraper(exchange_id="bitget")
    scraper.scrape_candles_to_csv(
        symbol="ETH/USDT",
        timeframe="1d",
        start_date_str="2021-01-01T00:00:00",
        end_date_str="2024-01-01T00:00:00",
        limit=100
        )       
