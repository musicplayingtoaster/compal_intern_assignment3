from typing import Annotated
from fastapi import FastAPI, Body, Form
import uvicorn

from ..resources.todo_model import Todo
from ..resources import producer
from ..resources import mq_keys

app = FastAPI()

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


# # app mount at the end, as if before the static file application will capture the request before the @app stuff does
# # also you can't put this in main() ig... weird...
# app.mount("/", StaticFiles(directory="src/assignment2/static", html=True), name="static")

def main() -> None:
#    database.init_todo_list()
    producer.init_publisher()
    # database.init_todo_list()
    uvicorn.run(app, host="0.0.0.0", port=8000) 

if __name__ == "__main__":
    main()
