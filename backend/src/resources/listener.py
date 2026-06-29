# base listener and helper functions
import aio_pika, asyncio
from .connections import connection_params_rabbitmq as conn_params
from . import mq_keys
from ..producer import producer

class Listener:
    def __init__(self):
        self.conn_params = conn_params
        self.connection = None
        self.queue = None
    
    async def connect(self):
        self.connection = await aio_pika.connect_robust(**self.conn_params)

    async def listen(self, key, process_message):
        if not self.connection:
            await self.connect()

        async with self.connection:
            channel = await self.connection.channel()
            await channel.set_qos(prefetch_count=1) # limits number of unack messages to 1

            self.queue = await channel.declare_queue(key, durable=True)

            await self.queue.consume(process_message)

            await asyncio.Future()
    

# helper function for listeners
async def publish_to_websockets(data):
    producer.init_publisher()
    producer.publish(routing_key=mq_keys.WS_KEY, data=data)