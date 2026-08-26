from pydantic import BaseModel

class OHLCV(BaseModel):
    timestamp: int
    open:float
    high:float
    low:float
    close:float
    volume_weighted_avg: float
    volume:float
    trade_count:int
    pair: str

class L2Book(BaseModel):
    price:float
    volume:float
    side:str
    pair:str
    timestamp:int

class GroupedBook(BaseModel):
    price: float
    qty: float
    side: str
    pair: str

class RecentTrades(BaseModel):
    price:float
    volume:float
    time:float
    side:str
    order_type:str
    misc:str
    trade_id:int
    pair:str

class RecentSpreads(BaseModel):
    time:int
    bid:float
    ask:float
    pair:str