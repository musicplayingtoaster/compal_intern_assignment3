import asyncio, json, aio_pika
from ..resources import mq_keys
from ..resources.todo_model import Todo
from ..resources.listener import Listener, publish_to_websockets
from ..database.database_accessor import DatabaseAccessor
from ..database import database

# routing keys
CREATE_KEY = mq_keys.CREATE_KEY

# exchange name
EXCHANGE = mq_keys.EXCHANGE

async def process_message(message: aio_pika.IncomingMessage):
    print("Create Listener Heard Message!")
    async with message.process():
        try:
            payload = json.loads(message.body.decode())

            async with database_accessor.get_pg_async_conn as conn_db, database_accessor.get_rdcache_async_conn as conn_cache:
                await database.add_todo(todo=Todo.model_validate(payload), conn_db=conn_db, conn_cache=conn_cache)
                print("Added to Database!")
            
            await publish_to_websockets((payload, CREATE_KEY))
            print("Published to Websockets!")
            
            await asyncio.sleep(0)
        except Exception as e:
            print(f"Failed to process message. Error: {e}")

async def create_listen():
    print("Create_Listener attempting to connect to RabbitMQ")
    listener = Listener()
    await listener.listen(key=CREATE_KEY, process_message=process_message)
        

database_accessor = DatabaseAccessor(create_listen)

async def main():
    shutdown_trigger = asyncio.Event()

    async with database_accessor:
        try:
            await shutdown_trigger.wait()
        except asyncio.CancelledError:
            print("Shutdown triggered via cancellation.")
        finally:
            print("System shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")