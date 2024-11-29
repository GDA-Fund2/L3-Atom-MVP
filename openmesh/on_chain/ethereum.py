from openmesh.feed import RPC
import asyncio
from yapic import json
from openmesh.chain import ChainFeed
import dataclasses

from openmesh.feed import AsyncFeed

import logging
from decimal import Decimal, getcontext

TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

getcontext().prec = 38

# Handles the conversions between hex digits and their decimal equivalents automatically


@dataclasses.dataclass
class EthereumObject:
    atomTimestamp: int

    def __post_init__(self):
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if field.type is int and isinstance(value, str):
                setattr(self, field.name, int(value, 16))
            if field.type is Decimal and isinstance(value, str):
                setattr(self, field.name, Decimal(int(value, 16)))
            if field.type is Decimal and isinstance(value, (float, int)):
                setattr(self, field.name, Decimal(value))
            if field.type is str and isinstance(value, int):
                setattr(self, field.name, hex(value))

    def to_dict(self):
        return dataclasses.asdict(self)

    def to_json_string(self):
        return json.dumps(self.to_dict())


@dataclasses.dataclass
class EthereumBlock(EthereumObject):
    baseFeePerGas: int
    number: int
    hash: str
    parentHash: str
    nonce: str
    sha3Uncles: str
    logsBloom: str
    transactionsRoot: str
    stateRoot: str
    receiptsRoot: str
    miner: str
    difficulty: int
    totalDifficulty: Decimal
    extraData: str
    size: int
    gasLimit: Decimal
    gasUsed: Decimal
    blockTimestamp: int


@dataclasses.dataclass
class EthereumTransaction(EthereumObject):
    blockTimestamp: int
    hash: str
    nonce: str
    blockHash: str
    blockNumber: int
    transactionIndex: int
    fromAddr: str
    toAddr: str
    value: Decimal
    gas: int
    gasPrice: int
    input: str
    type: str
    maxFeePerGas: int = None
    maxPriorityFeePerGas: int = None


@dataclasses.dataclass
class EthereumLog(EthereumObject):
    blockTimestamp: int
    blockNumber: int
    blockHash: str
    transactionIndex: int
    transactionHash: str
    logIndex: int
    address: str
    data: str
    topic0: str
    topic1: str = None
    topic2: str = None
    topic3: str = None


@dataclasses.dataclass
class EthereumTransfer(EthereumObject):
    blockTimestamp: int
    blockNumber: int
    blockHash: str
    transactionHash: str
    transactionIndex: int
    logIndex: int
    fromAddr: str
    toAddr: str
    tokenAddr: str
    value: Decimal


class Ethereum(ChainFeed):
    name = "ethereum"
    chain_objects = {
        'blocks': EthereumBlock,
        'transactions': EthereumTransaction,
        'logs': EthereumLog,
        'token_transfers': EthereumTransfer
    }

    event_objects = ['dex_trades']

    type_map = {
        '0x0': 'Legacy',
        '0x1': 'EIP-2930',
        '0x2': 'EIP-1559'
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_block_num = 0
        self.last_block_hash = None
        self.last_block_time = 0

    @classmethod
    def get_key(cls, msg: dict):
        if 'topic0' in msg:
            return f"{msg['topic0']};{msg['address']}".encode()
        return None

    async def subscribe(self, conn, feeds, symbols):
        blocks_res = await conn.make_call('eth_subscribe', ['newHeads'])
        logging.debug(
            "%s: Attempting to subscribe to newHeads with result %s", self.name, blocks_res)
        self.block_sub_id = blocks_res.get('result', None)
        logging.info(f"Subscribed to newHeads with id {self.block_sub_id}")

    def hex_to_int(self, hex_str: str):
        return int(hex_str, 16)

    async def get_transactions_by_block(self, conn: RPC, block_number: int):
        res = await conn.make_call('eth_getBlockByNumber', [hex(block_number), True])
        while 'result' not in res:
            await asyncio.sleep(1)
            res = await conn.make_call('eth_getBlockByNumber', [hex(block_number), True])
        return res['result']['transactions']

    async def get_block_by_number(self, conn: RPC, block_number: str):
        res = await conn.make_call('eth_getBlockByNumber', [block_number, True])
        while not res.get('result', None):
            await asyncio.sleep(1)
            res = await conn.make_call('eth_getBlockByNumber', [block_number, True])
        return res['result']

    async def get_logs_by_block_number(self, conn: RPC, block_number: str):
        res = await conn.make_call('eth_getLogs', [{
            'fromBlock': block_number,
            'toBlock': block_number
        }])
        while 'result' not in res:
            await asyncio.sleep(1)
            res = await conn.make_call('eth_getLogs', [{
                'fromBlock': block_number,
                'toBlock': block_number
            }])
        return res['result']

    async def _transactions(self, conn: RPC, transactions: list, ts: int):
        for transaction in transactions:
            del transaction['v']
            del transaction['r']
            del transaction['s']
            transaction.pop('chainId', None)
            transaction.pop('accessList', None)
            transaction.pop('yParity', None)
            transaction['fromAddr'] = transaction.pop('from')
            transaction['toAddr'] = transaction.pop('to')
            transaction['type'] = self.type_map[transaction['type']]
            transaction_obj = EthereumTransaction(
                **transaction, blockTimestamp=self.last_block_time, atomTimestamp=ts)
            logging.debug(f"Received transaction {transaction_obj.hash}")
            await self.kafka_backends['transactions'].write(transaction_obj.to_json_string())

    async def _log(self, conn: RPC, log: dict, ts: int):
        logging.debug("Received log")
        del log['removed']
        topics = {f'topic{i}': topic for i, topic in enumerate(log['topics'])}
        del log['topics']
        log_obj = EthereumLog(
            **log, **topics, blockTimestamp=self.last_block_time, atomTimestamp=ts)
        await self.kafka_backends['logs'].write(log_obj.to_json_string())

    async def _block(self, conn: RPC, block: dict, ts: int):
        logging.debug(
            "-----------------\n\n\nReceived block\n\n\n-----------------")
        del block['mixHash']
        del block['transactions']
        del block['uncles']
        if block.get('withdrawals') != None:
            del block['withdrawals']
            del block['withdrawalsRoot']
        block['blockTimestamp'] = self.hex_to_int(block.pop('timestamp')) * 1000
        block_obj = EthereumBlock(**block, atomTimestamp=ts)
        self.last_block_hash = block_obj.hash
        self.last_block_num = block_obj.number
        self.last_block_time = block_obj.blockTimestamp
        await self.kafka_backends['blocks'].write(block_obj.to_json_string())

    def _word_to_addr(self, word: str):
        if len(word) > 40:
            return word[-40:]
        return word

    async def _token_transfer(self, conn: RPC, transfer: dict, ts: int):
        logging.debug("Received token transfer")
        topics = transfer['topics']
        # If we don't at least have a from address, we don't care
        if len(topics) <= 1:
            return
        from_addr = self._word_to_addr(topics[1])
        to_addr = self._word_to_addr(topics[2])
        value = transfer['data'][:66]
        # Could be another Transfer event other than the standard ERC20 method. value is expected to be a 256 bit unsigned integer
        # https://github.com/OpenZeppelin/openzeppelin-contracts/blob/master/contracts/token/ERC20/ERC20.sol#L113
        if len(value) < 66:
            return
        msg = dict(
            tokenAddr=transfer['address'],
            transactionHash=transfer['transactionHash'],
            transactionIndex=transfer['transactionIndex'],
            blockNumber=transfer['blockNumber'],
            logIndex=transfer['logIndex'],
            blockHash=transfer['blockHash'],
            blockTimestamp=self.last_block_time,
            fromAddr=from_addr,
            toAddr=to_addr,
            value=value,
            atomTimestamp=ts
        )
        transferObj = EthereumTransfer(**msg)
        await self.kafka_backends['token_transfers'].write(transferObj.to_json_string())

    async def process_message(self, message: str, conn: AsyncFeed, timestamp: int):
        msg = json.loads(message)
        data = msg['params']
        # For some reason the subscription id can be truncated in the message, so we'll add a few checks to see if the message is a new block
        if data['subscription'] == self.block_sub_id or data['subscription'] in self.block_sub_id or 'number' in data['result']:
            block_number = data['result']['number']
            block = await self.get_block_by_number(self.http_node_conn, block_number)
            await self._block(conn, block.copy(), timestamp)
            await self._transactions(conn, block['transactions'], timestamp)
            logs = await self.get_logs_by_block_number(self.http_node_conn, block_number)
            for log in logs:
                if len(log.get("topics")) == 0:
                    continue
                topics = log['topics']
                await self._log(conn, log.copy(), timestamp)
                if topics[0].casefold() == TRANSFER_TOPIC:
                    await self._token_transfer(conn, log, timestamp)
        else:
            logging.warning(f"Received unknown message {msg}")

