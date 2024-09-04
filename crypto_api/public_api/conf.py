from pydantic_settings import BaseSettings


class Config(BaseSettings):
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0


def get_config():
    return Config()
