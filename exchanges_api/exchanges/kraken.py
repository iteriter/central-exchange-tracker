import asyncio
import json
from typing import override


from loguru import logger
from websockets.asyncio.client import connect


from exchanges_api.exchanges.base import BaseExchange, Listener
from exchanges_api.schemas.common import PairId, PriceUpdate
from exchanges_api.schemas.kraken import (
    InstrumentUpdateSchema,
    TickerUpdateSchema,
)


class Kraken(BaseExchange):
    def __init__(self, base_url, queue, max_streams=30):
        super().__init__(
            id="kraken",
            websockets_url=base_url,
            updates_queue=queue,
            max_per_connection=max_streams,
        )

    async def start(self):
        # load initial instruments
        await self._get_instruments(init=True)
        logger.debug(
            f"loaded {len(self.pairs)} online instruments for exchange '{self.id}'"
        )

        n_batches, batch_size = self._get_batches()
        await self.run_listeners(n_batches=n_batches, batch_size=batch_size)
        self.tasks.add(asyncio.create_task(self._get_instruments()))

    def get_listings(self) -> list[PairId]:
        return list(self.pairs)

    async def _get_instruments(self, init=False):
        async with connect(f"{self.base_url}") as ws:
            await ws.send(
                json.dumps(
                    {
                        "method": "subscribe",
                        "params": {"channel": "instrument", "snapshot": init},
                    }
                )
            )

            async for msg in ws:
                update = json.loads(msg)
                if update.get("channel") == "instrument" and update.get("type") in [
                    "snapshot",
                    "update",
                ]:
                    update = InstrumentUpdateSchema(**update)

                    for pair in update.pairs:
                        pair_id = await self._parse_pair_label(pair.symbol)

                        if pair.status == "online":
                            self._add_pair(pair_id)
                        else:
                            self._del_pair(pair_id)

                    if init:
                        logger.info(f"loaded {len(update.pairs)} online instruments")
                        return

    @override
    def _make_pair_label(self, id: PairId) -> str:
        """
        Kraken format is 'CXT/USD'
        """
        return f"{id.base}/{id.quote}".upper()

    @override
    async def _parse_pair_label(self, label: str) -> PairId:
        """
        Kraken format is 'CXT/USD'
        """
        base, quote = label.split("/")
        return PairId(base=base, quote=quote)

    @override
    async def parse_update(self, message: str | bytes) -> PriceUpdate | None:
        update = json.loads(message)

        if update.get("channel") == "ticker":
            update = TickerUpdateSchema(**update)

            logger.trace(f"got update with {len(update.data)} order(s)")
            for event in update.data:
                pair_id = await self._parse_pair_label(event.symbol)
                price = self.avg_price(event.best_ask, event.best_bid)

            return PriceUpdate(self.id, pair_id, price)

    @override
    async def subscribe(self, socket, pairs):
        tickers = [f"{self._make_pair_label(p)}" for p in pairs]
        await socket.send(
            json.dumps(
                {
                    "method": "subscribe",
                    "params": {
                        "channel": "ticker",
                        "symbol": tickers,
                        "snapshot": False,
                    },
                }
            )
        )

    @override
    async def run_listeners(self, n_batches, batch_size):
        for p in self.pairs:
            try:
                self.new_tickers.put_nowait(p)
            except asyncio.QueueFull:
                continue

        for i in range(n_batches):
            batch = [list(self.pairs)[i]]  # * batch_size : (i + 1) * batch_size]

            url = f"{self.base_url}"
            listener = Listener(
                exchange=self,
                capacity=batch_size,
                symbols=set(batch),
                tickers_queue=self.new_tickers,
            )

            new_task = asyncio.create_task(listener.listen(url))

            self.listeners.append(listener)
            self.tasks.add(new_task)
