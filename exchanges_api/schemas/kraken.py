from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, AliasPath


class TradeEventSchema(BaseModel):
    """
    example:
    {
        "symbol": "MATIC/USD",
        "side": "sell",
        "price": 0.5117,
        "qty": 40.0,
        "ord_type": "market",
        "trade_id": 4665906,
        "timestamp": "2023-09-25T07:49:37.708706Z"
    }
    """

    symbol: str
    side: Literal["buy", "sell"]
    price: float
    qty: float
    ord_type: Literal["market", "limit"]
    trade_id: int
    timestamp: datetime


class PairUpdateSchema(BaseModel):
    """
    example:
    {
        "symbol": "BTC/USD",
        "base": "BTC",
        "quote": "USD",
        "status": "online",
        "qty_precision": 8,
        "qty_increment": 1e-08,
        "price_precision": 1,
        "cost_precision": 5,
        "marginable": true,
        "has_index": true,
        "cost_min": 0.5,
        "margin_initial": 0.2,
        "position_limit_long": 250,
        "position_limit_short": 200,
        "tick_size": 0.1,
        "price_increment": 0.1,
        "qty_min": 0.0001
    }
    """

    symbol: str
    base: str
    quote: str
    status: str
    price_precision: int


class TickerEventSchema(BaseModel):
    symbol: str

    best_ask: float = Field(alias="ask")
    best_bid: float = Field(alias="bid")


class UpdateSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    channel: str
    type: str


class TickerUpdateSchema(UpdateSchema):
    data: list[TickerEventSchema] = []


class InstrumentUpdateSchema(UpdateSchema):
    pairs: list[PairUpdateSchema] = Field(
        [], validation_alias=AliasPath("data", "pairs")
    )


class TradeUpdateSchema(UpdateSchema):
    data: list[TradeEventSchema] = []
