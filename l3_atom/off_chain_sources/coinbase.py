import websockets
import asyncio
import time
import json

from normalise.coinbase_normalisation import NormaliseCoinbase
from helpers.read_config import get_symbols
#from sink_connector.kafka_producer import KafkaProducer
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect
from source_connector.restapi_calls import get_snapshot

book_url = 'wss://ws-feed.exchange.coinbase.com'
snapshot_url = 'https://api.exchange.coinbase.com/products/{}/book?level=3'

async def main():
    producer = RedisProducer("coinbase")
    symbols = get_symbols('coinbase')
    await connect(book_url, handle_coinbase, producer, symbols, True)

async def handle_coinbase(ws, producer, symbols, is_book):
    normalise = NormaliseCoinbase().normalise
    for symbol in symbols:
        subscribe_message = {
            "type": "subscribe",
            "channels": [{"name": "full", "product_ids": [symbol]}]
        }
        await ws.send(json.dumps(subscribe_message))
        # Snapshot is too large to produce as raw
        # snapshot = await get_snapshot(snapshot_url.format(symbol))
        # await produce_message(snapshot, raw_producer, normalised_producer, trades_producer, normalise)
    
    await produce_messages(ws, producer, normalise)

if __name__ == "__main__":
    asyncio.run(main())