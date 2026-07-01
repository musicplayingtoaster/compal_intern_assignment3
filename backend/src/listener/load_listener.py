import asyncio, aio_pika, uvicorn
from resources import mq_keys
from resources.listener import Listener, publish_to_websockets
from database import database_accessor 
from database import database
from fastapi import FastAPI
from contextlib import asynccontextmanager

# routing keys
LOAD_KEY = mq_keys.LOAD_KEY

# exchange name
EXCHANGE = mq_keys.EXCHANGE

async_db_context = asynccontextmanager(database_accessor.get_pg_async_conn)

async def process_message(message: aio_pika.IncomingMessage):
    print("Load Listener Heard Message!")
    async with message.process():
        try:
            async with async_db_context() as conn_db, await database_accessor.get_rdcache_async_conn as conn_cache:
                retrieved_todos = await database.retrieve_all_todos(conn_db=conn_db, conn_cache=conn_cache)
                print("Retrieved todos from Database!")
            
            
            await publish_to_websockets((retrieved_todos, LOAD_KEY))
            print("Published to Websockets!")
            
            await asyncio.sleep(0)
        except Exception as e:
            print(f"Failed to process message. Error: {e}")

async def load_listen():
    print("Load_Listener attempting to connect to RabbitMQ")
    listener = Listener()
    await listener.listen(key=LOAD_KEY, process_message=process_message)
        
app = FastAPI(lifespan=database_accessor.create_lifespan(rabbitmq_listener=load_listen))

# database_accessor = DatabaseAccessor(load_listen)

# async def init():
#     shutdown_trigger = asyncio.Event()

#     loop = asyncio.get_event_loop()

#     def helper_stop_signal():
#         print("Received shutdown signal from Docker...")
#         shutdown_trigger.set()

#     for sig in (signal.SIGTERM, signal.SIGINT):
#         loop.add_signal_handler(sig, helper_stop_signal)

#     async with database_accessor:
#         try:
#             await shutdown_trigger.wait()
#         except asyncio.CancelledError:
#             print("Shutdown triggered via cancellation.")
#         finally:
#             print("System shutdown complete.")

def main():
    # asyncio.run(init())
    uvicorn.run(app)

if __name__ == "__main__":
    main()