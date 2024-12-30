import os
import time

import ccxt
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('BITGET_API_KEY')
SECRET_KEY = os.getenv('BITGET_SECRET_KEY')
PASSWORD = os.getenv('BITGET_PASSWORD')


# Initialize the Bitget exchange
bitget = ccxt.bitget({
        'apiKey': API_KEY,
        'secret': SECRET_KEY,
        'password': PASSWORD,
    })

while True:

    order_book = bitget.fetch_order_book('BTC/USDT', limit=1)
    print(order_book['bids'])
    print(order_book['asks'])
    print("##")
    time.sleep(1)
