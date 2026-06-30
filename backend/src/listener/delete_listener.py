import asyncio, json, aio_pika
from resources import mq_keys
from resources.listener import Listener, publish_to_websockets
from database.database_accessor import DatabaseAccessor
from database import database

# routing keys
DELETE_KEY = mq_keys.DELETE_KEY

# exchange name
EXCHANGE = mq_keys.EXCHANGE

async def process_message(message: aio_pika.IncomingMessage):
    print("Delete Listener Heard Message!")
    async with message.process():
        try:
            payload = json.loads(message.body.decode()) # Primary Key

            async with database_accessor.get_pg_async_conn as conn_db, database_accessor.get_rdcache_async_conn as conn_cache:
                await database.remove_todo(primary_key=payload, conn_db=conn_db, conn_cache=conn_cache)
                print("Removed from Database!")
            
            await publish_to_websockets((payload, DELETE_KEY))
            print("Published to Websockets!")
            
            await asyncio.sleep(0)
        except Exception as e:
            print(f"Failed to process message. Error: {e}")

async def create_listen():
    print("Create_Listener attempting to connect to RabbitMQ")
    listener = Listener()
    await listener.listen(key=DELETE_KEY, process_message=process_message)
        

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