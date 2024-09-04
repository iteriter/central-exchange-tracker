import asyncio
import json
from typing import override

import httpx
from loguru import logger
from websockets import connect

from exchanges_api.schemas.common import PairId, PriceUpdate
from exchanges_api.exchanges.base import BaseExchange, Listener
from exchanges_api.schemas.binance import (
    MiniTickerEventSchema,
    TickerUpdateSchema,
)


class Binance(BaseExchange):
    """
    limits:

        - connection runs at most 24h
        - 5 pongs / json controll messages per second per connection
        - at most 1024 streams per connection
        - 300 connection attempts per 5 mins (i.e. 1 second per connection)
    """

    def __init__(self, base_url, queue, max_streams, split_evenly=True):
        super().__init__(
            id="binance",
            websockets_url=base_url,
            updates_queue=queue,
            max_per_connection=max_streams,
            split_evently=split_evenly,
        )

        self.tickers_to_pairs: dict[str, PairId] = {}

    @override
    def _make_pair_label(self, id: PairId) -> str:
        return f"{id.base}{id.quote}".lower()

    @override
    async def _parse_pair_label(self, label: str) -> PairId:
        if label in self.tickers_to_pairs:
            return self.tickers_to_pairs[label]

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://api.binance.com/api/v3/exchangeInfo?symbol={label}"
            )
            symbol = response.json()["symbols"][0]

        if not symbol["status"] == "TRADING":
            raise ValueError(f"Symbol {label} is not trading")

        id = PairId(base=symbol["baseAsset"], quote=symbol["quoteAsset"])

        return id

    @override
    async def run_listeners(self, n_batches, batch_size):
        for p in self.pairs:
            try:
                self.new_tickers.put_nowait(p)
            except asyncio.QueueFull:
                continue

        initial = list(self.pairs)[:n_batches]

        for _ in range(0, n_batches):
            initial_stream = initial.pop()
            url = f"{self.base_url}/stream?streams={self._make_pair_label(initial_stream)}@ticker"

            listener = Listener(
                exchange=self,
                capacity=batch_size,
                symbols=set([initial_stream]),
                tickers_queue=self.new_tickers,
            )

            new_task = asyncio.create_task(listener.listen(url))

            self.listeners.append(listener)
            self.tasks.add(new_task)

    @override
    async def start(self):
        # initially fetch all tickers via rest API
        await self._fetch_tickers_list()
        # get pair ids from lookup dict
        self.pairs = set(self.tickers_to_pairs.values())
        logger.info(f"loaded {len(self.tickers_to_pairs)} online instruments")

        # split tickers into batches and run listeners for each batch
        n_batches, batch_size = self._get_batches()
        await self.run_listeners(n_batches, batch_size)

        # start background task to refresh tickers
        self.tasks.add(asyncio.create_task(self._get_instruments()))

    async def _fetch_tickers_list(self):
        # get existing tickers via rest API
        # and create a lookup dict from label to pair id
        async with httpx.AsyncClient() as client:
            response = await client.get("https://api.binance.com/api/v3/exchangeInfo")

            self.tickers_to_pairs = {
                e["symbol"]: PairId(base=e["baseAsset"], quote=e["quoteAsset"])
                for e in response.json()["symbols"]
                if e["status"] == "TRADING"
            }

    async def _get_instruments(self):
        # subscribe to ticker updates
        async with connect(f"{self.base_url}/ws/!miniTicker@arr") as websocket:
            while True:
                response = json.loads(await websocket.recv())
                for e in response:
                    event = MiniTickerEventSchema(**e)

                    pair_id = await self._parse_pair_label(event.symbol)

                    if pair_id not in self.pairs:
                        self.new_tickers.put_nowait(pair_id)

                    self._add_pair(pair_id)

    @override
    async def parse_update(self, message):
        update = json.loads(message)

        if "data" not in update:
            return

        event = TickerUpdateSchema(**update).data
        pair_id = await self._parse_pair_label(event.symbol)
        price = self.avg_price(event.best_ask, event.best_bid)

        return PriceUpdate(self.id, pair_id, price)

    async def subscribe(self, socket, pairs):
        logger.debug("sending subscribe request")
        payload = json.dumps(
            {
                "method": "SUBSCRIBE",
                "params": [f"{self._make_pair_label(p)}@ticker" for p in pairs],
            }
        )
        await socket.send(payload)

    async def unsubscribe(self):
        pass

    @override
    async def get_listings(self) -> list[PairId]:
        return list(self.tickers_to_pairs.values())
