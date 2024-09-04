import os
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    KRAKEN_WS_URL: str = "wss://ws.kraken.com/v2"
    BINANCE_WS_URL: str = "wss://stream.binance.com:9443"


def get_config():
    return Config()
