import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets

from src import config
from src.events import OrderBookEvent, TradeEvent
from src.kafka_producer import EventProducer

logger = logging.getLogger(__name__)

EXCHANGE = "binance"


def _to_iso(ms_timestamp: int) -> str:
    return datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc).isoformat()


def _parse_trade(msg: dict) -> TradeEvent:
    return TradeEvent(
        exchange=EXCHANGE,
        symbol=msg["s"],
        trade_id=str(msg["t"]),
        price=float(msg["p"]),
        quantity=float(msg["q"]),
        # Binance's `m` flag means the buyer was the maker, so the taker
        # (the side that triggered the trade) was the seller.
        side="sell" if msg["m"] else "buy",
        timestamp=_to_iso(msg["E"]),
    )


def _parse_depth_update(msg: dict) -> OrderBookEvent:
    return OrderBookEvent(
        exchange=EXCHANGE,
        symbol=msg["s"],
        type="update",
        bids=msg.get("b", []),
        asks=msg.get("a", []),
        timestamp=_to_iso(msg["E"]),
    )


async def run(producer: EventProducer) -> None:
    """Connect to Binance's WebSocket feed and republish normalized events, forever."""
    subscribe_message = json.dumps(
        {
            "method": "SUBSCRIBE",
            "params": [f"{config.BINANCE_SYMBOL}@trade", f"{config.BINANCE_SYMBOL}@depth"],
            "id": 1,
        }
    )

    while True:
        try:
            async with websockets.connect(config.BINANCE_WS_URL) as ws:
                await ws.send(subscribe_message)
                logger.info("Binance connector subscribed to %s", config.BINANCE_SYMBOL)

                async for raw in ws:
                    msg = json.loads(raw)
                    event_type = msg.get("e")

                    if event_type == "trade":
                        await producer.send_trade(_parse_trade(msg))
                    elif event_type == "depthUpdate":
                        await producer.send_orderbook(_parse_depth_update(msg))
                    # Ignore subscription acks and anything else unrecognized.

        except (websockets.ConnectionClosed, OSError) as exc:
            logger.warning("Binance connector disconnected (%s), reconnecting...", exc)
            await asyncio.sleep(config.RECONNECT_DELAY_SECONDS)
