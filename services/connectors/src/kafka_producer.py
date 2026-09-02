import logging

from aiokafka import AIOKafkaProducer

from src import config
from src.events import OrderBookEvent, TradeEvent

logger = logging.getLogger(__name__)


class EventProducer:
    """Thin wrapper around AIOKafkaProducer for publishing normalized events."""

    def __init__(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=config.KAFKA_BROKER)

    async def start(self) -> None:
        await self._producer.start()
        logger.info("Kafka producer connected to %s", config.KAFKA_BROKER)

    async def stop(self) -> None:
        await self._producer.stop()

    async def send_trade(self, event: TradeEvent) -> None:
        await self._producer.send_and_wait(
            config.KAFKA_TOPIC_TRADES,
            key=event.kafka_key().encode("utf-8"),
            value=event.to_json(),
        )

    async def send_orderbook(self, event: OrderBookEvent) -> None:
        await self._producer.send_and_wait(
            config.KAFKA_TOPIC_ORDERBOOK,
            key=event.kafka_key().encode("utf-8"),
            value=event.to_json(),
        )
