from pathlib import Path
from fastapi import FastAPI, HTTPException

class Topic:
    def __init__(self, name: str, data_dir: str = "data"):
        self.name = name
        self.topic_dir = Path(data_dir) / name
        self.topic_dir.mkdir(parents=True, exist_ok=True)

    def get_cursor_file(self, consumer_id: str) -> Path:
        return self.topic_dir / f"{consumer_id}.cursor"

    def produce(self, message: str) -> int:
        """Writes a message to a topic's log and returns the message ID."""
        # Find the next message ID
        message_files = sorted(self.topic_dir.glob("*.msg"))
        next_id = 1
        if message_files:
            last_id = int(message_files[-1].stem)
            next_id = last_id + 1

        message_file = self.topic_dir / f"{next_id:08d}.msg"
        message_file.write_text(message)
        return next_id

    def consume(self, consumer_id: str):
        """Reads the next message for a consumer, based on their cursor."""
        cursor_file = self.get_cursor_file(consumer_id)
        last_read_id = 0
        if cursor_file.exists():
            last_read_id = int(cursor_file.read_text())

        message_files = sorted(self.topic_dir.glob("*.msg"))
        for message_file in message_files:
            message_id = int(message_file.stem)
            if message_id > last_read_id:
                return message_id, message_file.read_bytes()
        return None

    def commit(self, consumer_id, message_id):
        """Updates the consumer's cursor to the given message ID."""
        cursor_file = self.get_cursor_file(consumer_id)
        cursor_file.write_text(str(message_id))

app = FastAPI()

TOPICS = {}

@app.post("/topics/{topic_name}/create")
def create_topic(topic_name):
    if not topic_name in TOPICS:
        TOPICS[topic_name] = Topic(topic_name)

@app.post("/topics/{topic_name}/messages")
def publish_message(topic_name: str, message):
    """Receives a message from a producer and writes it to the topic's log."""
    if not topic_name in TOPICS:
        TOPICS[topic_name] = Topic(topic_name)
    topic = TOPICS[topic_name]
    message_offset = topic.produce(message)
    return {"message_id": message_offset}

@app.get("/topics/{topic_name}/messages")
def consume_message(topic_name: str, consumer_id: str):
    """Retrieves the next message for a consumer."""
    topic = TOPICS[topic_name]
    result = topic.consume(consumer_id)
    if not result:
        raise HTTPException(status_code=404, detail="No new messages")
    message_offset, message = result
    return {"message_id": message_offset, "data": message}

@app.post("/topics/{topic_name}/messages/{message_id}/ack")
def ack(topic_name, consumer_id, message_id):
    """Acknowledges a message, updating the consumer's cursor."""
    topic = TOPICS[topic_name]
    topic.commit(consumer_id, message_id)
    return {"status": "ok"}
