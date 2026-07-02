import asyncio, aio_pika, uvicorn, logging
from resources import mq_keys
from resources.listener import listener_manager, publish_to_websockets
from database import database_accessor 
from database import database
from fastapi import FastAPI
from contextlib import contextmanager

# routing keys
LOAD_KEY = mq_keys.LOAD_KEY

logger = logging.getLogger("uvicorn.error")

sync_db_context = contextmanager(database_accessor.get_pg_sync_conn)

async def process_message(message: aio_pika.IncomingMessage):
    print("Load Listener Heard Message!")
    async with message.process():
        try:
            with sync_db_context() as conn_db, database_accessor.get_rdcache_sync_conn() as conn_cache:
                retrieved_todos = database.retrieve_all_todos(conn_db=conn_db, conn_cache=conn_cache)
                print("Retrieved todos from Database!")
            
            
            await publish_to_websockets((retrieved_todos, LOAD_KEY))
            print("Published to Websockets!")
        except Exception as e:
            print(f"Failed to process message. Error: {e}")
            await message.reject()

async def load_listen():
    print("Load_Listener attempting to connect to RabbitMQ")
    logger.info("Load_Listener attempting to connect to RabbitMQ")
    listener = listener_manager
    await listener.listen(key=LOAD_KEY, process_message=process_message)
        
# app = FastAPI(lifespan=database_accessor.create_lifespan(load_listen))
app = FastAPI(lifespan=database_accessor.create_lifespan(load_listen))

def main():
    # asyncio.run(init())
    print("Load Starting...")
    uvicorn.run(app)

if __name__ == "__main__":
    main()