from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class PairId:
    base: str
    quote: str


@dataclass(slots=True, frozen=True)
class PriceUpdate:
    exchange_id: str
    pair_id: PairId
    price: float
