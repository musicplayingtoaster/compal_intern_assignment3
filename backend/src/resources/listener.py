# base listener
import aio_pika, asyncio
from .connections import connection_params_rabbitmq as conn_params

class Listener:
    async def __init__(self):
        self.connection = await aio_pika.connect_robust(**conn_params)
        self.queue = None
    
    async def listen(self, key, process_message:function):
        async with self.connection:
            channel = await self.connection.channel()
            await channel.set_qos(prefetch_count=1) # limits number of unack messages to 1

            self.queue = await channel.declare_queue(key, durable=True)

            await self.queue.consume(process_message)

            await asyncio.Future()
    


            