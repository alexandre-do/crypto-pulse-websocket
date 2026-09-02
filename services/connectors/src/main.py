import asyncio
import logging
import signal

from src.binance import connector as binance_connector
from src.coinbase import connector as coinbase_connector
from src.kafka_producer import EventProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    producer = EventProducer()
    await producer.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    tasks = [
        asyncio.create_task(binance_connector.run(producer), name="binance-connector"),
        asyncio.create_task(coinbase_connector.run(producer), name="coinbase-connector"),
    ]

    try:
        await stop_event.wait()
        logger.info("Shutdown signal received, stopping connectors...")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
