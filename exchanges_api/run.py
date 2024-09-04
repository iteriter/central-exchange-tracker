import asyncio
from collections import defaultdict
from statistics import mean

from loguru import logger

import redis
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

from exchanges_api.conf import get_config
from exchanges_api.exchanges.base import BaseExchange
from exchanges_api.exchanges.kraken import Kraken
from exchanges_api.exchanges.binance import Binance

from exchanges_api.schemas.common import PairId, PriceUpdate

config = get_config()


class PriceService:
    def __init__(self, exchanges: dict[str, BaseExchange], queue):
        self.exchanges = exchanges
        self.queue = queue

        """
        pairs = {
            'binance': {
                'btcusdt': {
                    'price': 100
                }
            },
            'kraken': {
                ...
            },
            [new default item]
        }
        """
        self.pairs_by_exchange: dict[str, dict[PairId, dict[str, float]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        self.redis = redis.Redis(
            host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB
        )
        indices = list(exchanges.keys()) + ["average"]
        self.redis.set("exchanges", ",".join(indices))

        for idx_id in indices:
            ft = self.redis.ft(f"idx:{idx_id}")
            try:
                ft.info()
            except Exception:
                ft.create_index(
                    fields=[
                        TextField("pair_id"),
                        NumericField("price"),
                        TextField("updated_at"),
                        TextField("precision"),
                    ],
                    definition=IndexDefinition(
                        prefix=[f"{idx_id}:"], index_type=IndexType.HASH
                    ),
                )

    def _get_exchange(self, exchange_id):
        if exchange_id not in self.exchanges:
            raise ValueError(f"Exchange {exchange_id} not found")
        return self.exchanges[exchange_id]

    async def start_all(self):
        tasks = []
        for exchange in self.exchanges.values():
            logger.info(f"starting parsing for {exchange.id}")
            tasks.append(asyncio.create_task(exchange.start()))

        await asyncio.gather(*tasks)

    async def start(self, exchange_id):
        await self.exchanges[exchange_id].start()

    async def stop(self, exchange_id):
        raise NotImplementedError
        # await self.exchanges[exchange_id].stop()

    async def consume(self):
        while True:
            update: PriceUpdate = await self.queue.get()

            self.pairs_by_exchange[update.exchange_id][update.pair_id][
                "price"
            ] = update.price

            self.update_pair_exchange_price(update)
            self.update_pair_price(update.pair_id)

    def update_pair_exchange_price(self, update):
        with self.redis.pipeline() as p:
            p.hset(
                # name=f"idx:{exchange_id}",
                name=f"{update.exchange_id}:{update.pair_id.base}{update.pair_id.quote}",
                mapping={
                    "pair_id": f"{update.pair_id.base}{update.pair_id.quote}",
                    "price": update.price,
                    "updated_at": "n/a",
                    "precision": "n/a",
                },
            )
            p.execute()

    def update_pair_price(self, pair):
        """
        Update the average price of pair across all exchanges
        """
        with self.redis.pipeline() as p:
            p.hset(
                f"average:{pair.base}{pair.quote}",
                mapping={
                    "pair_id": f"{pair.base}{pair.quote}",
                    "price": mean(
                        [
                            cex[pair]["price"]
                            for cex in self.pairs_by_exchange.values()
                            if cex[pair]
                        ]
                    ),
                },
            )
            p.execute()

    def get_exchange_listings(self, exchange_id):
        return self._get_exchange(exchange_id).get_listings()


async def main():
    queue = asyncio.Queue()
    exchanges: dict[str, BaseExchange] = {
        "kraken": Kraken(config.KRAKEN_WS_URL, max_streams=100, queue=queue),
        "binance": Binance(config.BINANCE_WS_URL, max_streams=200, queue=queue),
    }

    price_service = PriceService(exchanges, queue)

    async with asyncio.TaskGroup() as tg:
        fetching_task = tg.create_task(price_service.start_all())
        consuming_task = tg.create_task(price_service.consume())


if __name__ == "__main__":
    asyncio.run(main())
