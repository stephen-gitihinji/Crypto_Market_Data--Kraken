import pandas as pd
import asyncio
from ingestion import extract
from ingestion.schemas import OHLCV, L2Book, GroupedBook, RecentTrades, RecentSpreads
from ingestion import transformers 
from ingestion.staging import stage_data
import time

tickers = extract.get_tickers()

def ingest():
    #obtaining the data
    ohlcv_data = asyncio.run(extract.fetch_OHLCV_data(tickers))
    L2_book_data = asyncio.run(extract.fetch_L2_book(tickers))
    grouped_book_data = asyncio.run(extract.fetch_grouped_book(tickers))
    recent_trades_data = asyncio.run(extract.fetch_recent_trades(tickers))
    recent_spreads_data = asyncio.run(extract.fetch_recent_spreads(tickers))

    #transformations
    clean_ohlcv_data = [transformers.transform_OHLCV(record) for record in ohlcv_data]
    clean_l2_book = [record for ticker_sides in L2_book_data for record in transformers.transform_L2_book(ticker_sides)]
    clean_grouped_book = [record for ticker_sides in grouped_book_data for record in transformers.transform_grouped_book(ticker_sides)]
    clean_recent_trades = [record for ticker_sides in recent_trades_data for record in transformers.transform_recent_trades(ticker_sides)]
    clean_recent_spreads = [record for ticker_sides in recent_spreads_data for record in transformers.transform_recent_spreads(ticker_sides)]

    #validating
    valid_ohlcv = [OHLCV.model_validate(data).model_dump() for data in clean_ohlcv_data]
    valid_l2_book = [L2Book.model_validate(data).model_dump() for data in clean_l2_book]
    valid_grouped_book = [GroupedBook.model_validate(data).model_dump() for data in clean_grouped_book]
    valid_recent_trades = [RecentTrades.model_validate(data).model_dump() for data in clean_recent_trades]
    valid_recent_spreads = [RecentSpreads.model_validate(data).model_dump() for data in clean_recent_spreads]

    #staging the data
    stage_data(valid_ohlcv, "ohlcv_crypto_data")
    stage_data(valid_l2_book, "l2_book_data")
    stage_data(valid_grouped_book, "grouped_book_data")
    stage_data(valid_recent_trades, "recent_crypto_trades")
    stage_data(valid_recent_spreads, "recent_crypto_spreads")



if __name__ == "__main__":
    start_watch = time.perf_counter()
    ingest()
    print("Ingestion done")
    print(f"{time.perf_counter() - start_watch}s")