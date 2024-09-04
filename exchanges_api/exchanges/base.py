from abc import ABC, abstractmethod
import asyncio
from collections import defaultdict
import json
from typing import Callable

from loguru import logger
from websockets import connect

from exchanges_api.schemas.common import PairId, PriceUpdate


class BaseExchange(ABC):
    def __init__(
        self,
        id: str,
        websockets_url: str,
        updates_queue,
        max_per_connection: int,
        split_evently: bool = True,
    ):
        self.id = id
        self.active = False

        self.base_url = websockets_url
        self.queue = updates_queue

        self.pairs: set[PairId] = set()
        self.untracked_pairs: set[PairId] = set()
        self.max_per_connection = max_per_connection
        self.split_evently = split_evently

        self.new_tickers = asyncio.Queue()
        self.tasks = set()
        self.listeners = []

    def _get_batches(self):
        # split tickers into batches with separate connections
        n_items = len(self.pairs)
        n_batches = (
            max(n_items // self.max_per_connection, 1) + 1
        )  # avoid zero division

        logger.debug(f"splitting tickers into {n_batches} batches")

        if self.split_evently:
            # make batches of roughly equal size
            batch_size = n_items // n_batches
        else:
            # use maximum connections in each batch, last batch being smaller
            batch_size = self.max_per_connection

        return n_batches, batch_size

    def _add_pair(self, id: PairId):
        self.pairs.add(id)

    def _del_pair(self, id: PairId):
        if id in self.pairs:
            self.pairs.remove(id)

    @abstractmethod
    def start(self):
        """
        Start fetching price updates
        """
        raise NotImplementedError

    @abstractmethod
    def get_listings(self):
        """
        Return price of all trading pairs that are currently online
        """
        raise NotImplementedError

    @abstractmethod
    def _make_pair_label(self, base, quote) -> str:
        """
        Return an identifier for a trading pair in a format support by the exchange
        """
        raise NotImplementedError

    @abstractmethod
    async def _parse_pair_label(self, label: str) -> PairId:
        """
        Parse a trading pair label into a pair of base and quote
        """
        raise NotImplementedError

    def avg_price(self, ask, bid):
        """
        Update price for a trading pair
        """
        return (ask + bid) / 2

    @abstractmethod
    async def parse_update(self, message: str | bytes) -> PriceUpdate | None:
        raise NotImplementedError

    @abstractmethod
    async def run_listeners(self, pairs: list[str]):
        raise NotImplementedError

    def stream_update(self, update: PriceUpdate):
        try:
            self.queue.put_nowait(update)
        except asyncio.QueueFull:
            print("queue is full")

    @abstractmethod
    def subscribe(self, websocket, pairs: set[PairId]):
        raise NotImplementedError


class Listener:
    def __init__(
        self,
        exchange: BaseExchange,
        capacity: int,
        symbols: set[PairId],
        tickers_queue: asyncio.Queue,
    ) -> None:
        """
        :param: pairs_per_connection: how many pairs should be tracked per connection
        """
        self.exchange = exchange
        self.capacity = capacity

        self.tracked: set[PairId] = symbols
        self.pending: asyncio.Queue[PairId] = tickers_queue

    async def listen(self, url):
        """
        :param tracked: list of pairs to track
        """
        logger.debug(f"starting listener for {len(self.tracked)} pairs")

        async with connect(url) as websocket:
            await self.subscribe(websocket, [self.pending.get_nowait()])

            async for message in websocket:
                logger.debug(
                    f"tracking {len(self.tracked)} pairs, capacity: {self.capacity}, pending: {self.pending.qsize()}"
                )
                if len(self.tracked) < self.capacity:
                    logger.debug(
                        f"listener has {len(self.tracked)} tracked pairs out of {self.capacity}"
                    )

                    n_add = min(self.capacity - len(self.tracked), self.pending.qsize())
                    logger.debug(
                        f"adding {n_add} pairs (min of {self.capacity - len(self.tracked)}, {self.pending.qsize()})"
                    )
                    add_to_tracked = set()

                    try:
                        for _ in range(n_add):
                            add_to_tracked.add(self.pending.get_nowait())
                    except asyncio.QueueEmpty:
                        logger.debug("no more pairs to add")

                    await self.subscribe(websocket, add_to_tracked)

                update = await self.exchange.parse_update(message)
                if not update:
                    logger.trace(f"received message: {message}")
                    continue

                self.exchange.stream_update(update)

    async def subscribe(self, websocket, pairs):
        """
        :param tracked: list of pairs to track
        """
        # subscribe to new pairs to tracked if has capacity
        logger.debug(f"listener subscribing to {len(pairs)} new pairs")

        await self.exchange.subscribe(websocket, pairs)
        self.tracked = self.tracked.union(pairs)

        logger.debug(f"subscribed to {len(pairs)} new pairs")
