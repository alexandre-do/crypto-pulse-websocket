import os
from pathlib import Path

from dotenv import load_dotenv


def _load_repo_env() -> None:
    """Load the repo-root .env, if present, without overriding real env vars."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / ".env"
        if candidate.is_file():
            load_dotenv(dotenv_path=candidate)
            return


_load_repo_env()

BINANCE_WS_URL = os.environ.get("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws")
BINANCE_SYMBOL = os.environ.get("BINANCE_SYMBOL", "btcusdt")

COINBASE_WS_URL = os.environ.get("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
COINBASE_PRODUCT_ID = os.environ.get("COINBASE_PRODUCT_ID", "BTC-USD")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_TRADES = os.environ.get("KAFKA_TOPIC_TRADES", "trades")
KAFKA_TOPIC_ORDERBOOK = os.environ.get("KAFKA_TOPIC_ORDERBOOK", "orderbook")

# Seconds to wait before retrying a dropped WebSocket connection.
RECONNECT_DELAY_SECONDS = float(os.environ.get("CONNECTOR_RECONNECT_DELAY_SECONDS", "5"))
