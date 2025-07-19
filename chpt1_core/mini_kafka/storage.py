import os
from pathlib import Path

DATA_DIR = Path("data")

def get_topic_dir(topic: str) -> Path:
    """Returns the directory for a given topic, creating it if it doesn't exist."""
    topic_dir = DATA_DIR / topic
    topic_dir.mkdir(parents=True, exist_ok=True)
    return topic_dir

def get_cursor_file(topic: str, consumer_id: str) -> Path:
    """Returns the path to the cursor file for a consumer."""
    topic_dir = get_topic_dir(topic)
    return topic_dir / f"{consumer_id}.cursor"

def write_message(topic: str, data: bytes) -> int:
    """Writes a message to a topic's log and returns the message ID."""
    topic_dir = get_topic_dir(topic)
    
    # Find the next message ID
    message_files = sorted(topic_dir.glob("*.msg"))
    next_id = 1
    if message_files:
        last_id = int(message_files[-1].stem)
        next_id = last_id + 1
        
    message_file = topic_dir / f"{next_id:08d}.msg"
    message_file.write_bytes(data)
    return next_id

def read_next_message(topic: str, consumer_id: str) -> tuple[int, bytes] | None:
    """Reads the next message for a consumer, based on their cursor."""
    topic_dir = get_topic_dir(topic)
    cursor_file = get_cursor_file(topic, consumer_id)
    
    last_read_id = 0
    if cursor_file.exists():
        last_read_id = int(cursor_file.read_text())
        
    message_files = sorted(topic_dir.glob("*.msg"))
    
    for message_file in message_files:
        message_id = int(message_file.stem)
        if message_id > last_read_id:
            return message_id, message_file.read_bytes()
            
    return None

def acknowledge_message(topic: str, consumer_id: str, message_id: int):
    """Updates the consumer's cursor to the given message ID."""
    cursor_file = get_cursor_file(topic, consumer_id)
    cursor_file.write_text(str(message_id))