from typing import AsyncIterable
from l3_atom.orderbook_exchange import OrderBookExchange
from l3_atom.stream_processing.records import record_mapping


class Standardiser:
    """
    Base class for schema standardisers

    :param raw_topic: The raw topic to consume from
    :type raw_topic: str
    :param feeds: The feeds to standardise
    :type feeds: list
    :param exchange: The exchange to standardise for. Creates a connection to the exchange
    :type exchange: OrderBookExchange
    :param feed_to_record: Mapping of feed to record class
    :type feed_to_record: dict
    """
    raw_topic: str = NotImplemented
    feeds: list = NotImplemented
    exchange: OrderBookExchange = NotImplemented
    feed_to_record: dict = record_mapping

    def __init__(self) -> None:
        self.id = self.exchange.name
        self.normalised_topics = {
            f"{feed}": f"{self.id}_{feed}" for feed in self.feeds
        }
        self.raw_topic = f'{self.id}_raw'
        self.exchange = self.exchange()

    def normalise_symbol(self, exch_symbol: str) -> str:
        """Get the normalised symbol from the exchange symbol"""
        return self.exchange.get_normalised_symbol(exch_symbol)

    async def send_to_topic(self, feed: str, **kwargs):
        """
        Given a feed and arguments, send to the correct topic
        
        :param feed: The feed to send to
        :type feed: str
        :param kwargs: The arguments to use in the relevant Record
        :type kwargs: dict
        """
        val = self.feed_to_record[feed](**kwargs, exchange=self.id)
        val.validate()
        await self.normalised_topics[feed].send(
            value=val,
            key=kwargs['symbol']
        )

    async def handle_message(self, msg: dict):
        """
        Method to handle incoming messages. Overriden by subclasses.

        :param msg: The message to handle
        :type msg: dict
        """
        raise NotImplementedError

    async def process(self, stream: AsyncIterable) -> AsyncIterable:
        """
        Indefinite iterator over the stream of messages coming in over the raw topic. Continuously iterates and processes each incoming message.

        :param stream: The stream of messages to process
        :type stream: AsyncIterable
        :return: Iterator over the stream of raw processed messages
        :rtype: AsyncIterable
        """
        async for message in stream:
            await self.handle_message(message)
            yield message
