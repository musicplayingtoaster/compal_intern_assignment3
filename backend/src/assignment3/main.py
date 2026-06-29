from typing import Annotated
from fastapi import FastAPI, Body, WebSocket, WebSocketDisconnect, Depends, Form
from fastapi.staticfiles import StaticFiles
import uvicorn
from . import database, postgre_database, helper
from ..resources.todo_model import Todo
import json
import asyncio

from psycopg import Connection, AsyncConnection
import redis
import redis.asyncio as aioredis
import aio_pika

from ..producer.producer import producer
from ..resources import mq_keys


app = FastAPI(lifespan=helper.lifespan)

@app.post("/submit")
async def create_todo(data: Annotated[Todo, Form()]):
    producer.publish(routing_key=mq_keys.CREATE_KEY, data=data)
    return "Producer Published: CREATE"

@app.get("/load")
async def load_todos():
    producer.publish(routing_key=mq_keys.LOAD_KEY)
    return "Producer Published: LOAD"

@app.delete("/delete")
async def delete_todo(id: Annotated[int, Body()]):
    producer.publish(routing_key=mq_keys.DELETE_KEY, data=id)
    return "Producer Published: DELETE"

@app.put("/update") # Note: "todo" is empty. this is just for transfering data for resolved using the todo model
async def update_todo(data: Todo):
    producer.publish(routing_key=mq_keys.UPDATE_KEY, data=data)
    # database.update_todo(data.id, data.resolved)
    return "updated"


# Websocket stuff
# manager = helper.manager

# @app.websocket("/ws")
# async def handle_websockets(websocket: WebSocket, 
#                             channel:str = helper.CHANNEL_NAME, 
#                             conn_db: AsyncConnection = Depends(helper.get_pg_async_conn), 
#                             conn_cache: aioredis.Redis = Depends(helper.get_rdcache_async_conn)):
#     await manager.connect(websocket)
#     try:
#         connection = await helper.rabbitmq_connector()
#         channel = await connection.channel()
#         exchange = await channel.declare_exchange(helper.EXCHANGE_NAME, type="topic")

#         while True:
#             data = await websocket.receive_json()
#             recent = json.dumps(await create_todo(helper.Todo.model_validate(data), conn_db, conn_cache))
        
#             await exchange.publish(aio_pika.Message(body=recent.encode()), routing_key=helper.ROUTING_KEY)
#             await asyncio.sleep(0)

#     except WebSocketDisconnect:
#         manager.disconnect(websocket)
#     pass

# app mount at the end, as if before the static file application will capture the request before the @app stuff does
# also you can't put this in main() ig... weird...
app.mount("/", StaticFiles(directory="src/assignment2/static", html=True), name="static")

def main() -> None:
    postgre_database.init_todo_list()
    # database.init_todo_list()
    uvicorn.run(app, host="0.0.0.0", port=8000) 

if __name__ == "__main__":
    main()
