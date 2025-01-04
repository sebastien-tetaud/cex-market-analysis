from pathlib import Path

import ccxt
import pandas as pd
import time 

class OHLCVScraper:
    """
    A class to scrape OHLCV (Open, High, Low, Close, Volume) data from cryptocurrency exchanges using ccxt.
    """

    def __init__(self, path_save, exchange, exchange_id):
        """
        Initialize the scraper with the specified exchange.

        Args:
            exchange_id (str): The exchange ID (e.g., "binance", "bitget").
        """
        self.path = path_save
        self.exchange_id = exchange_id
        self.exchange = exchange

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
        start_timestamp = int(pd.Timestamp(start_date_str).timestamp() * 1000)
        end_timestamp = int(pd.Timestamp(end_date_str).timestamp() * 1000)

        # Fetch data in chunks of 'limit' timesteps
        data = []
        current_timestamp = start_timestamp
        max_retries = 3

        while current_timestamp < end_timestamp:
            retries = 0
            while retries < max_retries:
                try:
                    ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since=current_timestamp, limit=limit)
                    
                    if not ohlcv:
                        retries += 1
                        print(f"No data returned. Retrying {retries}/{max_retries}...")
                        time.sleep(1)  # Wait 1 second before retrying
                        continue
                    data.extend(ohlcv)
                    current_timestamp = ohlcv[-1][0] + 1  # Update timestamp to the last candle + 1ms
                    print(f"Fetched data up to {pd.to_datetime(current_timestamp, unit='ms')}")
                    time.sleep(0.5)
                    break
                except Exception as e:
                    print(f"An error occurred: {e}")
                    retries += 1
            if retries == max_retries:
                print(f"Max retries reached for timestamp {pd.to_datetime(current_timestamp, unit='ms')}. Skipping...")
                current_timestamp += 1 * 60 * 1000 

        # Create DataFrame if data exists
        if data:
            df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
            df['date'] = pd.to_datetime(df['date'], unit='ms')
            print(df)
        
        return df

    def write_to_csv(self, filename, df):
        """
        Save the DataFrame to a CSV file in a folder structure based on exchange ID and timeframe.

        Args:
            filename (str): The name of the CSV file.
            df (pd.DataFrame): The DataFrame to save.
            timeframe (str): The timeframe for candles (e.g., "1h", "1d").
        """
        # Create folder structure: ./data/<exchange_id>/<timeframe>/
        path = Path(self.path)
        path.mkdir(parents=True, exist_ok=True)
        full_path = path / filename

        # Save the DataFrame to the CSV file
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


def get_top_symbol_by_volume(exchange, pair_filter, top_n=100):
    """
    Fetch the top N cryptocurrencies traded against USDT based on 24h volume.

    Args:
        exchange_id (str): The exchange ID (e.g., "binance", "bitget").
        top_n (int): The number of top cryptocurrencies to fetch (default is 100).

    Returns:
        pd.DataFrame: A DataFrame containing the top N USDT pairs by 24h volume.
    """
    
    try:
        # Initialize the exchange
        # Fetch all tickers
        tickers = exchange.fetch_tickers()
        
        # Extract relevant data for USDT pairs
        data = []

        for symbol, ticker in tickers.items():
            if symbol.endswith(pair_filter):  # Filter for USDT pairs
                data.append({
                    "symbol": symbol,
                    "volume_24h": ticker.get("quoteVolume", 0),  # 24h trading volume in USDT
                    "price": ticker.get("last", 0),  # Last traded price
                })
        
        # Convert to DataFrame and sort by volume
        df = pd.DataFrame(data)
        df = df.sort_values(by="volume_24h", ascending=False).head(top_n)
        
        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()


def check_missing_timestamps(df, freq='1min'):
    """
    Checks for missing timestamps in a DataFrame with a datetime index.

    Parameters:
    ----------
    df : pd.DataFrame
        The input DataFrame with a 'date' column or a datetime index.
    freq : str, optional
        The frequency of the timestamps to check for (default is '1min').

    Returns:
    -------
    pd.DatetimeIndex
        A DatetimeIndex of missing timestamps. If no timestamps are missing, returns an empty DatetimeIndex.
    """
    # Ensure 'date' column is in datetime format if it exists
    if 'date' in df.columns:
        df = df.copy()  # Avoid modifying the original DataFrame
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

    # Ensure the index is a DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("The DataFrame index must be a DatetimeIndex or a 'date' column must exist.")

    # Generate a complete range of timestamps
    complete_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)

    # Identify missing timestamps
    missing_timestamps = complete_range.difference(df.index)

    return missing_timestamps