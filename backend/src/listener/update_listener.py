import asyncio, json, aio_pika, uvicorn, logging
from resources import mq_keys
from resources.listener import Listener, publish_to_websockets
from database import database_accessor
from database import database
from fastapi import FastAPI
from contextlib import asynccontextmanager

# routing keys
UPDATE_KEY = mq_keys.UPDATE_KEY

# exchange name
EXCHANGE = mq_keys.EXCHANGE

logger = logging.getLogger("uvicorn.error")


async_db_context = asynccontextmanager(database_accessor.get_pg_async_conn)

async def process_message(message: aio_pika.IncomingMessage):
    print("Update Listener Heard Message!")
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
            
            async with async_db_context() as conn_db, await database_accessor.get_rdcache_async_conn as conn_cache:
                await database.update_todo(primary_key=payload[0], resolved=payload[1], conn_db=conn_db, conn_cache=conn_cache)
                print("Updated Todo in Database!")
            
            # returns the primary key and resolved used to allow js to update
            await publish_to_websockets((payload, UPDATE_KEY)) 
            print("Published to Websockets!")
            await message.ack()
        except Exception as e:
            print(f"Failed to process message. Error: {e}")
            await message.reject()

async def update_listen():
    print("Update_Listener attempting to connect to RabbitMQ")
    logger.info("Update_Listener attempting to connect to RabbitMQ")
    listener = Listener()
    await listener.listen(key=UPDATE_KEY, process_message=process_message)

# app = FastAPI(lifespan=database_accessor.create_lifespan(update_listen))
app = FastAPI(lifespan=database_accessor.create_lifespan(update_listen))

def main():
    # asyncio.run(init())
    uvicorn.run(app)

if __name__ == "__main__":
    main()