import websockets
import asyncio
import time
import json

from normalise.dydx_normalisation import NormaliseDydx
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

url = 'wss://api.dydx.exchange/v3/ws'

async def main():
    producer = RedisProducer("dydx")
    symbols = get_symbols('dydx')
    await connect(url, handle_dydx, producer, symbols)

async def handle_dydx(ws, producer, symbols):
    for symbol in symbols:
        subscribe_message = {'type': 'subscribe', 
                'channel': 'v3_orderbook', 
                'id': symbol,
                'includeOffsets': True
            }
        await ws.send(json.dumps(subscribe_message))
        subscribe_message['channel'] = 'v3_trades'
        del subscribe_message['includeOffsets']
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, producer, NormaliseDydx().normalise)

if __name__ == "__main__":
    asyncio.run(main())