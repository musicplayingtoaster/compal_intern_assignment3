# base listener and helper functions
import aio_pika, asyncio
from .connections import connection_params_rabbitmq as conn_params
from . import mq_keys, producer

class Listener:
    def __init__(self):
        self.conn_params = conn_params
        self.connection = None
        self.channel = None
        self.exchange = None
    
    async def connect(self):
        if not self.connection:
            print("Attempting to Connect to RabbitMQ")
            self.connection = await aio_pika.connect_robust(**self.conn_params)
            print("Connected!")

            print("Getting Channel...")
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=10) # limits number of unack messages to 10
            print("Channel Created!")

            print("Declaring Exchange...")
            self.exchange = await self.channel.declare_exchange(name=mq_keys.EXCHANGE, type=aio_pika.ExchangeType.DIRECT)
            print("Exchange Declared!")

    async def listen(self, key, process_message, arguments = None):
        await self.connect()

        print("Declaring Queue...")
        queue = await self.channel.declare_queue(key, 
                                                 durable=True, 
                                                 arguments=arguments)
        print("Queue Declared!")

        print("Binding Queue...")
        await queue.bind(self.exchange, routing_key=key)
        print("Queue bound to Exchange and Key! Starting to Consume...")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                print("Message Recieved!")
                print(message)
                asyncio.create_task(process_message(message))
                print("Task created! Starting to Process...")
    
listener_manager = Listener()

# helper function for listeners
async def publish_to_websockets(data):
    await producer.init_publisher()
    await producer.publish(routing_key=mq_keys.WS_KEY, data=data)