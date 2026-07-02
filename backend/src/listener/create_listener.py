import asyncio, json, aio_pika, uvicorn, logging
from resources import mq_keys
from resources.todo_model import Todo
from resources.listener import listener_manager, publish_to_websockets
from database import database_accessor
from database import database
from fastapi import FastAPI
from contextlib import asynccontextmanager

# routing keys
CREATE_KEY = mq_keys.CREATE_KEY

logger = logging.getLogger("uvicorn.error")

async_db_context = asynccontextmanager(database_accessor.get_pg_async_conn)

async def process_message(message: aio_pika.IncomingMessage):
    print("Create Listener Heard Message!")
    logger.info("Create_Listener Heard Message!")
    async with message.process():
        try:
            payload = json.loads(message.body.decode())

            conn_cache = await database_accessor.get_rdcache_async_conn()
            async with async_db_context() as conn_db, conn_cache:
                await database.add_todo(todo=Todo.model_validate(payload), conn_db=conn_db, conn_cache=conn_cache)
                print("Added to Database!")
            
            await publish_to_websockets((payload, CREATE_KEY))
            print("Published to Websockets!")
        except Exception as e:
            print(f"Failed to process message. Error: {e}")
            await message.reject()

async def create_listen():
    print("Create_Listener attempting to connect to RabbitMQ")
    logger.info("Create_Listener attempting to connect to RabbitMQ")
    listener = listener_manager
    await listener.listen(key=CREATE_KEY, process_message=process_message)
        
# app = FastAPI(lifespan=database_accessor.create_lifespan(create_listen))
app = FastAPI(lifespan=database_accessor.create_lifespan(create_listen))

def main():
    # asyncio.run(init())
    uvicorn.run(app)

if __name__ == "__main__":
    main()