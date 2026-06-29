# Stuff to send to rabbitmq container
# Producer helper class for main.py to use (organization purposes)
import aio_pika
from ..resources.connections import connection_params_rabbitmq as conn_params
from ..resources import mq_keys
from contextlib import asynccontextmanager
from fastapi import FastAPI

# exchange name
EXCHANGE = mq_keys.EXCHANGE

rabbitmq_connection = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global rabbitmq_connection
    try:
        rabbitmq_connection = await aio_pika.connect_robust(**conn_params)
        print("Connected to RabbitMQ.")
        yield
    finally:
        if rabbitmq_connection:
            await rabbitmq_connection.close()
            print("RabbitMQ connection closed.")

class Producer:
    async def __init__(self):
        self.channel = await rabbitmq_connection.channel()
        self.exchange = await self.channel.declare_exchange(name=EXCHANGE, type=aio_pika.ExchangeType.FANOUT)

    async def publish(self, routing_key, data = None):
        await self.exchange.publish(message=aio_pika.Message(body=data.encode()), 
                                    routing_key=routing_key)
        

# import .producer into other scripts
producer = Producer()