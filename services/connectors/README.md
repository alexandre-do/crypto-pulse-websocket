# Connectors

Python (asyncio) services that subscribe to the Binance (BTC/USDT) and Coinbase (BTC-USD) WebSocket feeds, normalize trade and order book events, and publish them onto Kafka tagged by exchange and symbol. See the root [README](../../README.md#architecture) for how this fits into the pipeline.

## Layout

- `src/binance/connector.py` – Binance WebSocket client (`trade` + `depthUpdate` streams)
- `src/coinbase/connector.py` – Coinbase WebSocket client (`matches` + `level2` channels)
- `src/events.py` – normalized `TradeEvent` / `OrderBookEvent` schemas
- `src/kafka_producer.py` – shared Kafka producer (`aiokafka`)
- `src/config.py` – environment configuration (reads the repo-root `.env`)
- `src/main.py` – entrypoint; runs both connectors concurrently

## Running locally

```bash
pip install -r requirements.txt
python -m src.main
```

Requires Kafka reachable at `KAFKA_BROKER` (see the root `.env.example`).
