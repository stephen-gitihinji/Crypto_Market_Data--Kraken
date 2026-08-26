import pandas as pd

ohlcv_columns = ["timestamp","open","high","low","close","volume_weighted_avg","volume","trade_count","pair"]
l2_book_columns = ["price", "volume", "timestamp", "side", "pair"]
recent_trades_columns = ["price", "volume", "time", "side", "order_type", "misc", "trade_id", "pair"]
recent_spreads_columns = ["time", "bid", "ask", "pair"]

def add_pair(result):
    ticker_name = "".join(list(result.keys())[0])
    record_with_pair = [record + [ticker_name] for record in result[ticker_name]]
    return record_with_pair

def transform_OHLCV(ohlcv_record):
    extended_record = add_pair(ohlcv_record)[0]
    #inserting columns and creating a dict
    clean_record = dict(zip(ohlcv_columns, extended_record))
    return clean_record

def transform_L2_book(l2_record):
    ticker_name = "".join(list(l2_record.keys())[0])
    asks = [ask + ["bid", ticker_name] for ask in l2_record[ticker_name]['asks']]
    bids = [bid + ["ask", ticker_name] for bid in l2_record[ticker_name]['bids']]
    extended_L2_records = [*asks, *bids]
    clean_record = [dict(zip(l2_book_columns, record)) for record in extended_L2_records]
    return clean_record

def transform_grouped_book(grouped_record):
    bids = [bid | {"side":"bid", "pair":grouped_record['pair']} for bid in grouped_record['bids']]
    asks = [ask | {"side":"bid", "pair":grouped_record['pair']} for ask in grouped_record['asks']] 
    extended_record = [*bids, *asks]
    return extended_record

def transform_recent_trades(trades_record):
    extended_record = add_pair(trades_record)
    clean_record = [dict(zip(recent_trades_columns, record)) for record in extended_record]
    return clean_record

def transform_recent_spreads(spreads_record):
    extended_record = add_pair(spreads_record)
    clean_record = [dict(zip(recent_spreads_columns, record)) for record in extended_record]
    return clean_record
