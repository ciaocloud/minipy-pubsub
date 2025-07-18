
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pulsar_mini import storage

app = FastAPI()

class Message(BaseModel):
    data: str

@app.post("/topics/{topic}/messages")
def produce(topic: str, message: Message):
    """Receives a message from a producer and writes it to the topic's log."""
    message_id = storage.write_message(topic, message.data.encode("utf-8"))
    return {"message_id": message_id}

@app.get("/topics/{topic}/messages")
def consume(topic: str, consumer_id: str):
    """Retrieves the next message for a consumer."""
    result = storage.read_next_message(topic, consumer_id)
    if not result:
        raise HTTPException(status_code=404, detail="No new messages")
    
    message_id, data = result
    return {"message_id": message_id, "data": data.decode("utf-8")}

@app.post("/topics/{topic}/messages/{message_id}/ack")
def acknowledge(topic: str, message_id: int, consumer_id: str):
    """Acknowledges a message, updating the consumer's cursor."""
    storage.acknowledge_message(topic, consumer_id, message_id)
    return {"status": "ok"}
