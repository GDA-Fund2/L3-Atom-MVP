from typing import Union
from openmesh.helpers.read_config import get_secrets
import asyncio
from openmesh.data_source import DataFeed
import base64

from openmesh.feed import AsyncConnectionManager, HTTPRPC, WSRPC
from openmesh.sink_connector.kafka_multiprocessed import AvroKafkaConnector

import logging


class Chain:

    def load_node_conf(self):
        secrets = get_secrets()
        # maybe split this across multiple lines so we can easily parse it
        # from what i can gather it compares the secrets (really environment variables), with the name of the current chain if they match they are assigned to each other or something
        # [ 'ETH_WSS_URL' ], 'eth' if match then same
        return {k.split('_', maxsplit=1)[1].lower(): v for k, v in secrets.items() if k.startswith(self.name.upper())}


class ChainFeed(Chain, DataFeed):

    http_node_conn: Union[HTTPRPC, WSRPC] = NotImplemented
    # { <feed>: <feed object>}
    chain_objects: dict = NotImplemented

    # List of all the feeds related to individual contract events
    event_objects: list = NotImplemented

    # { <feed>: <Kafka Producer> }
    kafka_backends: dict = NotImplemented

    def __init__(self):
        super().__init__(max_syms=None)
        self.node_conf = self.load_node_conf()
        self._init_http_node_conn(**self.node_conf)
        self.kafka_backends = {}

        self.ws_rpc_endpoints = {
            self.node_conf['node_ws_url']: [
                self.name
            ]
        }

    def _init_http_node_conn(self, node_http_url=None, node_secret=None, **kwargs):
        self.http_node_conn = HTTPRPC(
            self.name, addr=node_http_url, auth_secret=node_secret)

    def _get_auth_header(self, username, password):
        assert ':' not in username
        auth = base64.b64encode(f'{username}:{password}'.encode()).decode()
        return ('Authorization', f'Basic {auth}')

    async def auth_ws(self, addr, options):
        return addr, {'extra_headers': [self._get_auth_header(username='', password=self.node_conf['node_secret'])]}

    def _init_kafka(self, loop: asyncio.AbstractEventLoop):
        logging.info('%s: Starting Kafka connectors', self.name)
        for feed, feed_obj in self.chain_objects.items():
            logging.info('%s: Starting Kafka connector for %s',
                         self.name, feed)
            self.kafka_backends[feed] = AvroKafkaConnector(
                self, topic=f"{self.name}_{feed}", record=feed_obj)
        list(self.kafka_backends.values())[0].create_chain_topics(
            self.chain_objects, self.event_objects, self.name)
        for backend in self.kafka_backends.values():
            backend.start(loop)

    def start(self, loop: asyncio.AbstractEventLoop):
        """
        Start WS connections for raw chain data

        :param loop: Event loop to run the connection on
        :type loop: asyncio.AbstractEventLoop
        """
        self._init_kafka(loop)
        rest_connections = self._init_rest()
        auth = None
        if 'node_secret' in self.node_conf:
            auth = self.auth_ws
        for connection in rest_connections:
            self.connection_handlers.append(AsyncConnectionManager(
                connection, None, self.process_message, None, None, self.retries, self.interval, self.timeout, self.delay))
        for (endpoint, channels) in self.ws_rpc_endpoints.items():
            connection = WSRPC(
                self.name, addr=endpoint, authentication=auth)
            self.connection_handlers.append(AsyncConnectionManager(
                connection, self.subscribe, self.process_message, None, channels, self.retries, self.interval, self.timeout, self.delay))

        for handler in self.connection_handlers:
            handler.start_connection(loop)

        logging.info('%s: Starting connection to %s chain',
                     self.name, self.name)
