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
        self.active_connections: list[WebSocket] = []
    
    # connection needs to be async as it requires waiting to ensure the websocket connection from client is successful
    async def connect(self, websocket:WebSocket):
        await websocket.accept()
        print("connected:", websocket)
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket:WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, data):
        for connection in self.active_connections:
            try:
                print("attempting to send data:", data, "to ", connection)
                await connection.send_json(data)
            except Exception:
                pass

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
        pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)

def main():
    # asyncio.run(init())
    uvicorn.run(app, host="0.0.0.0", port=8765)
    
if __name__ == "__main__":
    main()