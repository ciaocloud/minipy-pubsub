from pydantic import BaseModel


class Record(BaseModel):
    key: str | None = None
    value: str
    topic: str
    partition: int = 0
    offset: int