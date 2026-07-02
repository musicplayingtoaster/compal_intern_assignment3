# rabbitmq listener sends stuff here
# dedicated websocket server to push to all clients
import asyncio, json, aio_pika, uvicorn
from resources import mq_keys
from resources.listener import listener_manager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager

# routing keys
WS_KEY = mq_keys.WS_KEY

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(broadcaster())
    yield

app = FastAPI(lifespan=lifespan)

class ConnectionManager:
    def __init__(self):
        self.active_connections: set[WebSocket] = set()
        self.connection_ready = asyncio.Event()
    
    # connection needs to be async as it requires waiting to ensure the websocket connection from client is successful
    async def connect(self, websocket:WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        print("connected:", websocket)

        self.connection_ready.set()
        
    async def disconnect(self, websocket:WebSocket):
        self.active_connections.discard(websocket)
        print("disconnected:", websocket)

    async def broadcast(self, data):
        if not self.active_connections:
            return
        
        if json.loads(data)[1] == mq_keys.LOAD_KEY:
            self.connection_ready.clear()
        
        await self.connection_ready.wait()

        tasks = [connection.send_text(data) for connection in self.active_connections]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for connection, result in zip(list(self.active_connections), results):
            if isinstance(result, Exception):
                print(f"Broadcast failed for {connection.client}: {result}")
                self.active_connections.discard(connection)

manager = ConnectionManager()

async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
            
            print(payload)

            # Javascript will recieve a tuple of (data, action)
            # Take the action in Javascript to determine what to do with said data
            await manager.broadcast(payload)
        except Exception as e:
            print(f"Failed to process message. Error: {e}")

async def broadcaster():
    print("Broadcaster attempting to connect to RabbitMQ")
    listener = listener_manager
    await listener.listen(key=WS_KEY, process_message=process_message, arguments={"x-message-ttl": 60000 })

@app.websocket("/ws")
async def handle_websockets(websocket:WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        print("Disconnected Normally")
        await manager.disconnect(websocket)
    except Exception as e:
        print("bruuuuuuuuuuuuh exception:", e)
        await manager.disconnect(websocket)

def main():
    # asyncio.run(init())
    uvicorn.run(app, host="0.0.0.0", port=8765)
    
if __name__ == "__main__":
    main()