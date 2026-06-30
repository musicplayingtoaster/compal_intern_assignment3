import asyncio, json, aio_pika, signal
from resources import mq_keys
from resources.listener import Listener, publish_to_websockets
from database.database_accessor import DatabaseAccessor
from database import database

# routing keys
UPDATE_KEY = mq_keys.UPDATE_KEY

# exchange name
EXCHANGE = mq_keys.EXCHANGE

async def process_message(message: aio_pika.IncomingMessage):
    print("Update Listener Heard Message!")
    async with message.process():
        try:
            payload = json.loads(message.body.decode())

            async with database_accessor.get_pg_async_conn as conn_db, database_accessor.get_rdcache_async_conn as conn_cache:
                await database.update_todo(primary_key=payload[0], resolved=payload[1], conn_db=conn_db, conn_cache=conn_cache)
                print("Updated Todo in Database!")
            
            # returns the primary key and resolved used to allow js to update
            await publish_to_websockets((payload, UPDATE_KEY)) 
            print("Published to Websockets!")
            
            await asyncio.sleep(0)
        except Exception as e:
            print(f"Failed to process message. Error: {e}")

async def create_listen():
    print("Create_Listener attempting to connect to RabbitMQ")
    listener = Listener()
    await listener.listen(key=UPDATE_KEY, process_message=process_message)
        

database_accessor = DatabaseAccessor(create_listen)

async def init():
    shutdown_trigger = asyncio.Event()

    loop = asyncio.get_event_loop()
    
    def helper_stop_signal():
        print("Received shutdown signal from Docker...")
        shutdown_trigger.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, helper_stop_signal)

    async with database_accessor:
        try:
            await shutdown_trigger.wait()
        except asyncio.CancelledError:
            print("Shutdown triggered via cancellation.")
        finally:
            print("System shutdown complete.")

def main():
    asyncio.run(init())

if __name__ == "__main__":
    main()