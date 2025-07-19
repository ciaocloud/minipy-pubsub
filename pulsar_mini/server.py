from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pulsar_mini import storage

app = FastAPI()

class Message(BaseModel):
    data: str

# In a real system, this would be managed and persisted
TOPIC_METADATA = {
    "my-topic": {"partitions": 4},
    "another-topic": {"partitions": 1}
}

@app.post("/topics/{topic}/{partition}/messages")
def produce(topic: str, partition: int, message: Message):
    """Receives a message from a producer and writes it to the topic's partition log."""
    if topic not in TOPIC_METADATA or partition >= TOPIC_METADATA[topic]["partitions"]:
        raise HTTPException(status_code=404, detail="Topic or partition not found")
    message_id = storage.write_message(topic, partition, message.data.encode("utf-8"))
    return {"message_id": message_id}

@app.get("/topics/{topic}/{partition}/messages")
def consume(topic: str, partition: int, consumer_id: str):
    """Retrieves the next message for a consumer from a specific partition."""
    if topic not in TOPIC_METADATA or partition >= TOPIC_METADATA[topic]["partitions"]:
        raise HTTPException(status_code=404, detail="Topic or partition not found")
    result = storage.read_next_message(topic, partition, consumer_id)
    if not result:
        # Return 204 No Content, a more appropriate response than 404
        return Response(status_code=204)
    
    message_id, data = result
    return {"message_id": message_id, "data": data.decode("utf-8")}

@app.post("/topics/{topic}/{partition}/messages/{message_id}/ack")
def acknowledge(topic: str, partition: int, message_id: int, consumer_id: str):
    """Acknowledges a message, updating the consumer's cursor for a partition."""
    if topic not in TOPIC_METADATA or partition >= TOPIC_METADATA[topic]["partitions"]:
        raise HTTPException(status_code=404, detail="Topic or partition not found")
    storage.acknowledge_message(topic, partition, consumer_id, message_id)
    return {"status": "ok"}

@app.get("/topics/{topic}")
def get_topic_metadata(topic: str):
    """Gets the metadata for a topic, including the number of partitions."""
    if topic not in TOPIC_METADATA:
        raise HTTPException(status_code=404, detail="Topic not found")
    return TOPIC_METADATA[topic]