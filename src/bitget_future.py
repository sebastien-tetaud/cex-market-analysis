import csv
import os
import sys
from pathlib import Path

import ccxt
from dotenv import load_dotenv

# Add the parent directory to the system path
parent_dir = Path().resolve().parent
sys.path.append(str(parent_dir))

from utils.general import (OHLCVScraper, check_missing_timestamps,
                           get_top_symbol_by_volume)

# Load environment variables
load_dotenv()
root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(''))))
sys.path.append(root + '/python')
# Print the ccxt library version
print(f"ccxt version: {ccxt.__version__}")

API_KEY = os.getenv('BITGET_API_KEY')
SECRET_KEY = os.getenv('BITGET_SECRET_KEY')
PASSWORD = os.getenv('BITGET_PASSWORD')
MARKET_TYPE = "future"
EXCHANGE_ID = "bitget"
PATH_SAVE = f"/home/ubuntu/project/finance/cex-market-analysis/src/data/{EXCHANGE_ID}/{MARKET_TYPE}/new/"
TIMEFRAME = "1m"
FROM_DATE_STR = "2024-01-01 00:00:00"
exchange = getattr(ccxt, EXCHANGE_ID)({
'apiKey': API_KEY,
'secret': SECRET_KEY,
'password': PASSWORD,
'options': {
    'defaultType': MARKET_TYPE},
    'enableRateLimit': True
})


def retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
    num_retries = 0
    try:
        num_retries += 1
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
        # print('Fetched', len(ohlcv), symbol, 'candles from', exchange.iso8601 (ohlcv[0][0]), 'to', exchange.iso8601 (ohlcv[-1][0]))
        return ohlcv
    except Exception:
        if num_retries > max_retries:
            raise  # Exception('Failed to fetch', timeframe, symbol, 'OHLCV in', max_retries, 'attempts')


def scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
    earliest_timestamp = exchange.milliseconds()
    timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
    timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
    timedelta = limit * timeframe_duration_in_ms
    all_ohlcv = []
    while True:
        fetch_since = earliest_timestamp - timedelta
        ohlcv = retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, fetch_since, limit)
        print(len(ohlcv))
        # if we have reached the beginning of history
        if len(ohlcv)>0:
            if ohlcv[0][0] >= earliest_timestamp:
                break
        else:
            break
        earliest_timestamp = ohlcv[0][0]
        all_ohlcv = ohlcv + all_ohlcv
        print(len(all_ohlcv), symbol, 'candles in total from', exchange.iso8601(all_ohlcv[0][0]), 'to', exchange.iso8601(all_ohlcv[-1][0]))
        # if we have reached the checkpoint
        if fetch_since < since:
            break
    return all_ohlcv


def write_to_csv(filename, path_save, data):
    # Create the full path
    full_path = Path(path_save) / filename
    full_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
    # Write to the file
    with full_path.open('w+', newline='') as output_file:
        csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(data)


def scrape_candles_to_csv(filename, exchange_id, max_retries, symbol, timeframe, since, limit, path_save):
    # Instantiate the exchange by id
    exchange = getattr(ccxt, exchange_id)({
        'enableRateLimit': True,
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'password': PASSWORD,
        'options': {'defaultType': MARKET_TYPE},
    })
    # Convert `since` to milliseconds if needed
    if isinstance(since, str):
        since = exchange.parse8601(since)
    # Preload all markets
    exchange.load_markets()
    # Fetch all candles
    ohlcv = scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, limit)
    # Save them to CSV
    write_to_csv(filename, path_save, ohlcv)
    print(f"Saved {len(ohlcv)} candles from {exchange.iso8601(ohlcv[0][0])} to {exchange.iso8601(ohlcv[-1][0])} to {filename}")


if __name__ == "__main__":
    

    df_symbols = get_top_symbol_by_volume(exchange=exchange, pair_filter="/USDT:USDT", top_n=100)
    df_symbols = df_symbols.reset_index(drop=True)
    
    for symbol in df_symbols['symbol']:
        try:
            filename = symbol.replace("/", "_") + f"_{TIMEFRAME}.csv"
            scrape_candles_to_csv(filename, EXCHANGE_ID, 3, symbol, TIMEFRAME, FROM_DATE_STR, 100, PATH_SAVE)
        except Exception as e:
            print(f"Failed to scrape data for {symbol}: {e}")