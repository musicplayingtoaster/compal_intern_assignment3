# Stuff to send to rabbitmq container
# Producer helper class for main.py to use (organization purposes)
import aio_pika
from ..resources.connections import connection_params_rabbitmq as conn_params
from ..resources import mq_keys
import json

# exchange name
EXCHANGE = mq_keys.EXCHANGE

_pub_connection = None
_pub_exchange = None

async def init_publisher():
    global _pub_connection, _pub_exchange
    if _pub_connection is None:
        _pub_connection = await aio_pika.connect_robust(**conn_params)
        channel = await _pub_connection.channel()
        _pub_exchange = await channel.declare_exchange(
            name=mq_keys.EXCHANGE, 
            type=aio_pika.ExchangeType.DIRECT
        )

async def publish(routing_key, data = None):
    await _pub_exchange.publish(message=aio_pika.Message(body=json.dumps(data).encode()), 
                                routing_key=routing_key)