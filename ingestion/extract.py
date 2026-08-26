import requests as req
from datetime import datetime
import json
import asyncio
import aiohttp

TICKERS_URL = "https://api.kraken.com/0/public/Ticker?asset_class=forex&assetVersion=1"
OHLCV_BASE_URL = "https://api.kraken.com/0/public/OHLC"
L2_BOOK_BASE_URL = "https://api.kraken.com/0/public/Depth"
GROUPED_BOOK_URL = "https://api.kraken.com/0/public/GroupedBook"
RECENT_TRADES_URL = "https://api.kraken.com/0/public/Trades"
RECENT_SPREADS_URL = "https://api.kraken.com/0/public/Spread"

#get a list of 100 tickers from kraken
def get_tickers():
    tickers_response = req.get(TICKERS_URL).json()
    tickers = list(tickers_response['result'].keys())[:100]
    return tickers

 #calculate the "since" parameter(unix time) to be the start of the day
start_of_the_day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_of_the_day_epoch = int(start_of_the_day.timestamp())

#decorator function to extract the async_data and generate sessions
def extract_data(func):
    async def wrapper(all_tickers):
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(*[func(session, ticker) for ticker in all_tickers])
            return results
    return wrapper

#extract OHLCV information
@extract_data
async def fetch_OHLCV_data(session, ticker):
    OHLCV_parameters = {
        "interval" : json.dumps(1440),
        "since": json.dumps(start_of_the_day_epoch),
        "pair": ticker
    }
    async with session.get(OHLCV_BASE_URL, params=OHLCV_parameters) as response:
        response = await response.json()
    return response['result']

#extract L2 order book
@extract_data
async def fetch_L2_book(session, ticker):
    async with session.get(L2_BOOK_BASE_URL, params={"pair":ticker, "count":10}) as response:
        response = await response.json()
    return response['result']

#extract grouped order book
@extract_data
async def fetch_grouped_book(session, ticker):
    parameters={"pair":ticker, "depth":10, "grouping":1000}
    async with session.get(GROUPED_BOOK_URL, params=parameters) as response:
        response = await response.json()
    return response['result']

#extract recent trades
@extract_data
async def fetch_recent_trades(session, ticker):
    parameters = {
        "pair":ticker,
        "count":json.dumps(100),
        "since": json.dumps(start_of_the_day_epoch)
    }
    async with session.get(RECENT_TRADES_URL, params=parameters) as response:
        response = await response.json()
    return response['result']

#extract recent spreads
@extract_data
async def fetch_recent_spreads(session, ticker):
    parameters = {
        "pair":ticker,
        "since":start_of_the_day_epoch
    }
    async with session.get(RECENT_SPREADS_URL, params=parameters) as response:
        response = await response.json()
    return response['result']

