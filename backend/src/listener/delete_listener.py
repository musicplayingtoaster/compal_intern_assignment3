import asyncio, json, aio_pika, uvicorn, logging
from resources import mq_keys
from resources.listener import listener_manager, publish_to_websockets
from database import database_accessor
from database import database
from fastapi import FastAPI
from contextlib import asynccontextmanager

# routing keys
DELETE_KEY = mq_keys.DELETE_KEY

logger = logging.getLogger("uvicorn.error")

async_db_context = asynccontextmanager(database_accessor.get_pg_async_conn)

async def process_message(message: aio_pika.IncomingMessage):
    print("Delete Listener Heard Message!")
    async with message.process():
        try:
            payload = json.loads(message.body.decode()) # Primary Key
            print("Payload:", payload)

            # stuck here vvv (never enters)
            try:
                print("postgres connection...")
                async with async_db_context() as conn_db:
                    print("postgres connected! cache connection...")
                    conn_cache = await database_accessor.get_rdcache_async_conn()
                    async with conn_cache:
                        print("Removing from Database...")
                        await database.remove_todo(primary_key=payload, conn_db=conn_db, conn_cache=conn_cache)
                        print("Removed from Database!")
            except Exception as e:
                print("Exception Happened", e)
            
            await publish_to_websockets((payload, DELETE_KEY))
            print("Published to Websockets!")
        except Exception as e:
            print(f"Failed to process message. Error: {e}")
            await message.reject()

async def delete_listen():
    print("Delete_Listener attempting to connect to RabbitMQ")
    logger.info("Delete_Listener attempting to connect to RabbitMQ")
    listener = listener_manager
    await listener.listen(key=DELETE_KEY, process_message=process_message)
        
# app = FastAPI(lifespan=database_accessor.create_lifespan(delete_listen))
app = FastAPI(lifespan=database_accessor.create_lifespan(delete_listen))

def main():
    # asyncio.run(init())
    uvicorn.run(app)

if __name__ == "__main__":
    main()