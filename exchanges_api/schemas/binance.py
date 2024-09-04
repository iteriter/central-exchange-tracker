from datetime import datetime
from pydantic import ConfigDict, BaseModel, Field


class MiniTickerEventSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    symbol: str = Field(alias="s")

    event_type: str = Field(alias="e")
    event_time: int = Field(alias="E")
    close_price: float = Field(alias="c")
    open_price: float = Field(alias="o")
    high_price: float = Field(alias="h")
    low_price: float = Field(alias="l")
    total_traded_base_asset_volume: float = Field(alias="v")
    total_traded_quote_asset_volume: float = Field(alias="q")


class TickerEventSchema(MiniTickerEventSchema):
    model_config = ConfigDict(extra="ignore")

    event_time: datetime = Field(alias="E")

    best_bid: float = Field(validation_alias="b")
    best_ask: float = Field(validation_alias="a")


class TickerUpdateSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    stream: str
    data: TickerEventSchema


class TradeEventSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    symbol: str = Field(alias="s")
    price: float = Field(alias="p")
    quantity: float = Field(alias="q")

    event_type: str = Field(alias="e")
    event_time: int = Field(alias="E")
    trade_id: int = Field(alias="t")
    trade_time: int = Field(alias="T")
    is_buyer_maker: bool = Field(alias="m")
