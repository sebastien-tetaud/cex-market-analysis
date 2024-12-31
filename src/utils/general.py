from pathlib import Path

import ccxt
import pandas as pd


class OHLCVScraper:
    """
    A class to scrape OHLCV (Open, High, Low, Close, Volume) data from cryptocurrency exchanges using ccxt.
    """

    def __init__(self, exchange_id):
        """
        Initialize the scraper with the specified exchange.

        Args:
            exchange_id (str): The exchange ID (e.g., "binance", "bitget").
        """
        self.exchange_id = exchange_id
        self.exchange = getattr(ccxt, exchange_id)({'enableRateLimit': True})

    def fetch_ohlcv(self, symbol, timeframe, since, limit):
        """
        Fetch OHLCV data from the exchange.

        Args:
            symbol (str): The trading pair symbol (e.g., "BTC/USDT").
            timeframe (str): The timeframe for candles (e.g., "1h", "1d").
            since (int): The starting timestamp in milliseconds.
            limit (int): The maximum number of candles to fetch.

        Returns:
            list: A list of OHLCV data.
        """
        try:
            return self.exchange.fetch_ohlcv(symbol, timeframe, since, limit)
        except Exception as e:
            raise Exception(f"Failed to fetch {timeframe} {symbol} OHLCV: {e}")

    def scrape_ohlcv(self, symbol, timeframe, start_date_str, end_date_str, limit):
        """
        Scrape OHLCV data over a specified date range.

        Args:
            symbol (str): The trading pair symbol (e.g., "BTC/USDT").
            timeframe (str): The timeframe for candles (e.g., "1h", "1d").
            start_date_str (str): The start date in ISO 8601 format (e.g., "2022-01-01T00:00:00").
            end_date_str (str): The end date in ISO 8601 format (e.g., "2023-01-01T00:00:00").
            limit (int): The maximum number of candles to fetch per request.

        Returns:
            pd.DataFrame: A DataFrame containing the scraped OHLCV data.
        """
        start_date = pd.Timestamp(start_date_str)
        end_date = pd.Timestamp(end_date_str)

        start_date_ms = int(start_date.timestamp() * 1000)
        end_date_ms = int(end_date.timestamp() * 1000)

        all_ohlcv = []
        current_timestamp = start_date_ms

        while current_timestamp < end_date_ms:
            try:
                ohlcv = self.fetch_ohlcv(symbol, timeframe, current_timestamp, limit)

                if not ohlcv:
                    print(f"No data returned for {self.exchange.iso8601(current_timestamp)}. Skipping to the next timeframe.")
                    timeframe_duration_ms = self.exchange.parse_timeframe(timeframe) * 1000
                    current_timestamp += timeframe_duration_ms * limit
                    continue

                all_ohlcv += ohlcv
                print(f"{len(all_ohlcv)} {symbol} candles in total from {self.exchange.iso8601(all_ohlcv[0][0])} to {self.exchange.iso8601(all_ohlcv[-1][0])}")

                last_date = pd.Timestamp(ohlcv[-1][0], unit='ms')
                timedelta = last_date - pd.Timestamp(ohlcv[-2][0], unit='ms') if len(ohlcv) > 1 else pd.Timedelta(self.exchange.parse_timeframe(timeframe), unit='ms')
                current_timestamp = int((last_date + timedelta).timestamp() * 1000)

            except Exception as e:
                print(f"Error while fetching data: {e}")
                break

        final_df = pd.DataFrame(all_ohlcv, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        final_df['date'] = pd.to_datetime(final_df['date'], unit='ms')
        return final_df

    def write_to_csv(self, filename, df):
        """
        Save the DataFrame to a CSV file.

        Args:
            filename (str): The name of the CSV file.
            df (pd.DataFrame): The DataFrame to save.
        """
        path = Path("./data/raw/", self.exchange_id)
        path.mkdir(parents=True, exist_ok=True)
        full_path = path / filename

        df.to_csv(full_path, index=False)
        print(f"Saved data to {full_path}")

    def scrape_candles_to_csv(self, symbol, timeframe, start_date_str, end_date_str, limit):
        """
        Scrape OHLCV data and save it to a CSV file.

        Args:
            symbol (str): The trading pair symbol (e.g., "BTC/USDT").
            timeframe (str): The timeframe for candles (e.g., "1h", "1d").
            start_date_str (str): The start date in ISO 8601 format (e.g., "2022-01-01T00:00:00").
            end_date_str (str): The end date in ISO 8601 format (e.g., "2023-01-01T00:00:00").
            limit (int): The maximum number of candles to fetch per request.
        """
        df = self.scrape_ohlcv(symbol, timeframe, start_date_str, end_date_str, limit)
        sanitized_symbol = symbol.replace("/", "_")
        filename = f"{sanitized_symbol}_{timeframe}.csv"
        self.write_to_csv(filename, df)


# Example Usage
# if __name__ == "__main__":
#     scraper = OHLCVScraper(exchange_id="binance")
#     scraper.scrape_candles_to_csv(
#         symbol="BTC/USDT",
#         timeframe="1m",
#         start_date_str="2021-01-01T00:00:00",
#         end_date_str="2024-01-01T00:00:00",
#         limit=100
#     )
