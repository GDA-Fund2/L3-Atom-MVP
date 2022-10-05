import websockets
import asyncio
import time
import json

from normalise.okex_normalisation import NormaliseOkex
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

url = "wss://ws.okex.com:8443/ws/v5/public"

async def main():
    producer = RedisProducer("okex")
    symbols = get_symbols('okex')
    await connect(url, handle_okex, producer, symbols)

async def handle_okex(ws, producer, symbols):
    for symbol in symbols:
        subscribe_message = {}
        subscribe_message['op'] = 'subscribe'
        subscribe_message['args'] = [{"channel": "books", "instId": symbol}]
        await ws.send(json.dumps(subscribe_message))
        subscribe_message['args'] = [{"channel": "trades", "instId": symbol}]
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, producer, NormaliseOkex().normalise)


if __name__ == "__main__":
    asyncio.run(main())