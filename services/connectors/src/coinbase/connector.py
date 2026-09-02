import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets

from src import config
from src.events import OrderBookEvent, TradeEvent
from src.kafka_producer import EventProducer

logger = logging.getLogger(__name__)

EXCHANGE = "coinbase"


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _parse_match(msg: dict) -> TradeEvent:
    return TradeEvent(
        exchange=EXCHANGE,
        symbol=msg["product_id"],
        trade_id=str(msg["trade_id"]),
        price=float(msg["price"]),
        quantity=float(msg["size"]),
        side=msg["side"],
        timestamp=msg.get("time", _now_iso()),
    )


def _parse_snapshot(msg: dict) -> OrderBookEvent:
    return OrderBookEvent(
        exchange=EXCHANGE,
        symbol=msg["product_id"],
        type="snapshot",
        bids=msg.get("bids", []),
        asks=msg.get("asks", []),
        timestamp=_now_iso(),
    )


def _parse_l2update(msg: dict) -> OrderBookEvent:
    bids = [[price, size] for side, price, size in msg.get("changes", []) if side == "buy"]
    asks = [[price, size] for side, price, size in msg.get("changes", []) if side == "sell"]
    return OrderBookEvent(
        exchange=EXCHANGE,
        symbol=msg["product_id"],
        type="update",
        bids=bids,
        asks=asks,
        timestamp=msg.get("time", _now_iso()),
    )


async def run(producer: EventProducer) -> None:
    """Connect to Coinbase's WebSocket feed and republish normalized events, forever."""
    subscribe_message = json.dumps(
        {
            "type": "subscribe",
            "product_ids": [config.COINBASE_PRODUCT_ID],
            "channels": ["matches", "level2"],
        }
    )

    while True:
        try:
            async with websockets.connect(config.COINBASE_WS_URL) as ws:
                await ws.send(subscribe_message)
                logger.info("Coinbase connector subscribed to %s", config.COINBASE_PRODUCT_ID)

                async for raw in ws:
                    msg = json.loads(raw)
                    msg_type = msg.get("type")

                    if msg_type == "match":
                        await producer.send_trade(_parse_match(msg))
                    elif msg_type == "snapshot":
                        await producer.send_orderbook(_parse_snapshot(msg))
                    elif msg_type == "l2update":
                        await producer.send_orderbook(_parse_l2update(msg))
                    elif msg_type == "error":
                        logger.error("Coinbase feed error: %s", msg)
                    # Ignore subscription acks and anything else unrecognized.

        except (websockets.ConnectionClosed, OSError) as exc:
            logger.warning("Coinbase connector disconnected (%s), reconnecting...", exc)
            await asyncio.sleep(config.RECONNECT_DELAY_SECONDS)
