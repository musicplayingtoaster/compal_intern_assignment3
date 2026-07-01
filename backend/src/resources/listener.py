# base listener and helper functions
import aio_pika, asyncio
from .connections import connection_params_rabbitmq as conn_params
from . import mq_keys, producer

class Listener:
    def __init__(self):
        self.conn_params = conn_params
        self.connection = None
        self.channel = None
        self.queue = None
    
    async def connect(self):
        if not self.connection:
            print("Attempting to Connect to RabbitMQ")
            self.connection = await aio_pika.connect_robust(**self.conn_params)
            print("Connected!")

    async def listen(self, key, process_message):
        await self.connect()
        
        print("Getting Channel...")
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1) # limits number of unack messages to 1
        print("Channel Created!")

        print("Awaiting Queue...")
        self.queue = await self.channel.declare_queue(key, durable=True)
        print("Queue Declared! Starting to Consume Messages...")

        await self.queue.consume(process_message)
        print("Consuming Started!")

        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            print("Listener stopped.")
            await self.channel.close()
            await self.connection.close()
    

# helper function for listeners
async def publish_to_websockets(data):
    await producer.init_publisher()
    await producer.publish(routing_key=mq_keys.WS_KEY, data=data)