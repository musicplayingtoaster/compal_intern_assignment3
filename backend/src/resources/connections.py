# connection paramters
import os
from typing import Dict, Callable, Any

connection_params_db = {
    "host": os.environ.get('DB_HOST'), 
    "port": os.environ.get('DB_PORT'),
    "dbname": os.environ.get('DB_DATABASE'),
    "user": os.environ.get('DB_USER'),
    "password": os.environ.get('DB_PASSWORD'),
}

connection_params_redis_cache = {
    "host": os.environ.get('RDC_HOST'),
    "port": os.environ.get('RDC_PORT'),
    "db": 0,
    "decode_responses": True,
}

connection_params_rabbitmq = {
    "host": os.environ.get('RBMQ_HOST'),
    "port": int(os.environ.get('RBMQ_PORT')),
}

# copied from 
# https://stackoverflow.com/questions/74167054/how-can-i-implement-fastapi-like-depends-without-using-any-package-or-using-ra
# fastapi depends without fastapi
class ItWouldBeNice:
    cache: Dict[Callable, Any] = {}

    def __init__(self, dependency: Callable):
        self.dependency = dependency

    def __call__(self) -> Any:
        if self.dependency in ItWouldBeNice.cache:
            return ItWouldBeNice.cache[self.dependency]

        result = self.dependency()
        ItWouldBeNice.cache[self.dependency] = result
        return result