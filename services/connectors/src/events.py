import json
from dataclasses import asdict, dataclass, field
from typing import Literal


@dataclass
class TradeEvent:
    exchange: str
    symbol: str
    trade_id: str
    price: float
    quantity: float
    side: Literal["buy", "sell"]
    timestamp: str  # ISO 8601, UTC

    def kafka_key(self) -> str:
        return f"{self.exchange}:{self.symbol}"

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


@dataclass
class OrderBookEvent:
    exchange: str
    symbol: str
    type: Literal["snapshot", "update"]
    bids: list[list[str]] = field(default_factory=list)  # [[price, quantity], ...]
    asks: list[list[str]] = field(default_factory=list)
    timestamp: str = ""  # ISO 8601, UTC

    def kafka_key(self) -> str:
        return f"{self.exchange}:{self.symbol}"

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")
