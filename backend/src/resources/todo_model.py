# Universal Todo Model
from pydantic import BaseModel

class Todo(BaseModel):
    id: int | None = None
    todo: str
    resolved: int = 0