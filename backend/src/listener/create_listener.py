import asyncio, json, aio_pika
from ..resources import mq_keys
from ..resources.todo_model import Todo
from ..resources.listener import Listener, publish_to_websockets
from ..database.database_accessor import DatabaseAccessor
from ..database import database
from ..resources.connections import ItWouldBeNice, connection_params_rabbitmq as conn_params

# routing keys
CREATE_KEY = mq_keys.CREATE_KEY

# exchange name
EXCHANGE = mq_keys.EXCHANGE

async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
            
            # send payload to websocket server then await database.add_todo?
            # or send after to ensure parity

            await database.add_todo(Todo.model_validate(payload), 
                              ItWouldBeNice(database_accessor.get_pg_async_conn), 
                              ItWouldBeNice(database_accessor.get_rdcache_async_conn),)
            
            await publish_to_websockets((payload, CREATE_KEY))
            
            await asyncio.sleep(0)
        except Exception as e:
            print(f"Failed to process message. Error: {e}")

async def create_listen():
    print("Create_Listener attempting to connect to RabbitMQ")
    listener = Listener()
    await listener.listen(key=CREATE_KEY, process_message=process_message)
        

database_accessor = DatabaseAccessor(create_listen) # change this to create listen and fix stuff

async def main():
    async with database_accessor:
        create_listen()
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")