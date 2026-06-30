# rabbitmq listener sends stuff here
# dedicated websocket server to push to all clients
import asyncio, aio_pika, websockets, signal
from ..resources import mq_keys
from ..resources.listener import Listener

# routing keys
WS_KEY = mq_keys.WS_KEY

class WebSocketBroadcastServer:
    def __init__(self, host="localhost", port=8765):
        self.host = host
        self.port = port
        # Track active client connections
        self.connections = set()

    async def register(self, websocket):
        self.connections.add(websocket)
        print(f"Client connected. Total clients: {len(self.connections)}")
        try:
            await websocket.wait_closed()
        finally:
            self.connections.remove(websocket)
            print(f"Client disconnected. Total clients: {len(self.connections)}")

    async def broadcast(self, message):
        if self.connections:
            # Create send tasks for all active clients
            tasks = [client.send(message) for client in self.connections]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def start(self):
        async with websockets.serve(self.register, self.host, self.port):
            print(f"Broadcast server running on ws://{self.host}:{self.port}")
            await asyncio.Future()  


_websocket_manager:WebSocketBroadcastServer = None

async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            payload = message.body.decode()
            
            # Javascript will recieve a tuple of (data, action)
            # Take the action in Javascript to determine what to do with said data
            _websocket_manager.broadcast(payload)

            await asyncio.sleep(0)
        except Exception as e:
            print(f"Failed to process message. Error: {e}")

async def create_listen():
    print("Create_Listener attempting to connect to RabbitMQ")
    listener = Listener()
    await listener.listen(key=WS_KEY, process_message=process_message)

async def init():
    print("initiating...")
    shutdown_trigger = asyncio.Event()
    
    global _websocket_manager 
    _websocket_manager = WebSocketBroadcastServer(host="0.0.0.0", port=8765)
    await _websocket_manager.start()
    print(f"WebSocket server started on ws://{_websocket_manager.host}:{_websocket_manager.port}")

    rabbitmq_task = asyncio.create_task(create_listen())

    loop = asyncio.get_event_loop()

    def helper_stop_signal():
        print("Received shutdown signal from Docker...")
        shutdown_trigger.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, helper_stop_signal)


    try:
        await shutdown_trigger.wait()
    finally:
        print("Shutting down listener...")
        rabbitmq_task.cancel()
        await asyncio.gather(rabbitmq_task, return_exceptions=True)
        print("System shutdown complete.")
        

def main():
    asyncio.run(init())

if __name__ == "__main__":
    main()