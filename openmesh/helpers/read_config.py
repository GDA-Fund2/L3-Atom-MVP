from configparser import ConfigParser
import json
import sys
import os
from dotenv import dotenv_values

config_path = "config.ini"
env_path = "keys/.env"


def get_kafka_config():
    config = ConfigParser()
    config.read(config_path)
    return {
        **dotenv_values(env_path),
        **config['KAFKA']
    }


# TODO XXX: this is misleading AF delete or explain
def get_ethereum_provider():
    secrets = dotenv_values(env_path) | os.environ

    return {
        "eth_node_ws_url": secrets["ETHEREUM_NODE_WS_URL"],
        "eth_node_http_url": secrets["ETHEREUM_NODE_HTTP_URL"],
        "eth_node_secret": secrets["ETHEREUM_NODE_SECRET"]
    }


def get_secrets():
    # NOTE(Tomas): Adding os.environ here so we can set it from helm and avoid headaches
    return dotenv_values(env_path) | os.environ



def get_redis_config():
    config = ConfigParser()
    config.read(config_path)
    return {
        **config['REDIS'],
        **dotenv_values(env_path)
    }
