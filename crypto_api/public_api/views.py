import re

import redis
from django.http import JsonResponse
from redis.commands.search.query import Query

from public_api.conf import get_config

config = get_config()

redis = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)


def get_exchanges():
    """
    Get supported exchanges
    """
    r = redis.get("exchanges")
    exchanges = r.decode().split(",")
    return exchanges


def list_exchanges(request):
    return JsonResponse({"exchanges": get_exchanges()})


def get_all():
    result = {}
    for ex in get_exchanges():
        index = redis.ft(index_name=f"idx:{ex}")
        r = index.search(Query("*").paging(0, 10000).sort_by("pair_id"))
        result[ex] = {d.pair_id: d.price for d in r.docs}

    return result


def get_pair(pair_id):
    result = {}
    for ex in get_exchanges():
        r = redis.hget(f"{ex}:{pair_id}", "price")
        if r:
            result[ex] = r.decode()

    return {"result": result}


def get_exchange_pairs(exchange_id):
    index = redis.ft(index_name=f"idx:{exchange_id}")
    res = index.search(Query("*").paging(0, 10000).sort_by("pair_id"))

    return {
        "result": [{"pair_id": d.pair_id, "price": d.price} for d in res.docs],
    }


def get_pair_on_exchange(pair_id, exchange_id):
    r = redis.hget(f"{exchange_id}:{pair_id}", "price")
    return {"pair": pair_id, "price": r.decode()}


def validate_pair_id(pair_id: str):
    if not pair_id:
        raise ValueError("Pair ID cannot be empty")

    if not re.match(r"^[A-Z0-9]{1,4}[A-Z0-9]{1,4}$", pair_id):
        raise ValueError(
            "Pair ID must be uppercase alphanumeric, in format BASEQUOTE, e.g. BTCETH"
        )

    return pair_id


def validate_exchange_id(exchange_id: str):
    if not exchange_id:
        raise ValueError("Exchange ID cannot be empty")


def fetch(request):
    pair_id = request.GET.get("pair")
    exchange_id = request.GET.get("exchange")

    if pair_id:
        pair_id = validate_pair_id(pair_id)

    if pair_id and exchange_id:
        r = get_pair_on_exchange(pair_id=pair_id, exchange_id=exchange_id)
    elif pair_id:
        r = get_pair(pair_id)
    elif exchange_id:
        r = get_exchange_pairs(exchange_id)
    else:
        r = get_all()

    return JsonResponse(r)
