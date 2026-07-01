import asyncio, json, aio_pika, uvicorn
from resources import mq_keys
from resources.todo_model import Todo
from resources.listener import Listener, publish_to_websockets
from database import database_accessor
from database import database
from fastapi import FastAPI
from contextlib import asynccontextmanager

# routing keys
CREATE_KEY = mq_keys.CREATE_KEY

# exchange name
EXCHANGE = mq_keys.EXCHANGE

async_db_context = asynccontextmanager(database_accessor.get_pg_async_conn)

async def process_message(message: aio_pika.IncomingMessage):
    print("Create Listener Heard Message!")
    async with message.process():
        try:
            payload = message.body.decode()

            async with async_db_context() as conn_db, await database_accessor.get_rdcache_async_conn as conn_cache:
                await database.add_todo(todo=Todo.model_validate_json(payload), conn_db=conn_db, conn_cache=conn_cache)
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
        
app = FastAPI(lifespan=database_accessor.create_lifespan(rabbitmq_listener=create_listen))

def main():
    # asyncio.run(init())
    uvicorn.run(app)

if __name__ == "__main__":
    main()