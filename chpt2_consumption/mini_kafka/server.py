import time
import hashlib
from collections import defaultdict
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from mini_kafka import storage

app = FastAPI()

# --- In-Memory State (for this simple implementation) ---

# Topic Metadata
TOPIC_METADATA = {
    "my-topic": {"partitions": 4},
    "another-topic": {"partitions": 1}
}

# Consumer Group Management
# {group_id: {consumer_id: {partitions: [], last_seen: timestamp}}}
CONSUMER_GROUPS = defaultdict(dict)

# {group_id: {partition: consumer_id}}
PARTITION_ASSIGNMENTS = defaultdict(dict)

# --- Models ---

class Message(BaseModel):
    key: str | None = None
    data: str

class SubscriptionRequest(BaseModel):
    consumer_id: str
    group_id: str

# --- Helper Functions ---

def rebalance(group_id: str):
    """Assigns partitions to consumers in a group. A simple, centralized rebalance."""
    consumers = list(CONSUMER_GROUPS[group_id].keys())
    if not consumers:
        PARTITION_ASSIGNMENTS[group_id] = {}
        return

    # For now, we only support rebalancing the first topic found for the group
    # A real implementation would handle multiple topics per group.
    topic = "my-topic" # Simplified for this example
    num_partitions = TOPIC_METADATA[topic]["partitions"]
    partitions = list(range(num_partitions))

    assignments = {}
    for i, partition in enumerate(partitions):
        consumer = consumers[i % len(consumers)]
        assignments[partition] = consumer
    
    PARTITION_ASSIGNMENTS[group_id] = assignments
    print(f"Rebalanced group '{group_id}'. New assignments: {assignments}")

def get_partition_for_key(key: str, num_partitions: int) -> int:
    """A simple, deterministic partitioner based on the hash of the key."""
    hash_bytes = hashlib.sha256(key.encode("utf-8")).digest()
    hash_int = int.from_bytes(hash_bytes, 'big')
    return hash_int % num_partitions

# --- API Endpoints ---

@app.post("/topics/{topic}/messages")
def produce(topic: str, message: Message):
    """Receives a message and routes it to a partition."""
    if topic not in TOPIC_METADATA:
        raise HTTPException(status_code=404, detail="Topic not found")

    num_partitions = TOPIC_METADATA[topic]["partitions"]
    partition = 0
    if message.key:
        partition = get_partition_for_key(message.key, num_partitions)
    else:
        # Simple round-robin for non-keyed messages (in a real system, this would be more sophisticated)
        partition = int(time.time()) % num_partitions

    message_id = storage.write_message(topic, partition, message.data.encode("utf-8"))
    return {"message_id": message_id, "partition": partition}

@app.post("/subscriptions/subscribe")
def subscribe(req: SubscriptionRequest):
    """Registers a consumer with a group and triggers a rebalance."""
    group = CONSUMER_GROUPS[req.group_id]
    if req.consumer_id not in group:
        print(f"Consumer '{req.consumer_id}' joining group '{req.group_id}'")
    
    group[req.consumer_id] = {"last_seen": time.time()}
    rebalance(req.group_id)
    return {"status": "subscribed", "assignments": PARTITION_ASSIGNMENTS[req.group_id]}

@app.get("/messages")
def consume(group_id: str, consumer_id: str):
    """Fetches the next message for a consumer based on its group's assignments."""
    if group_id not in PARTITION_ASSIGNMENTS or consumer_id not in CONSUMER_GROUPS[group_id]:
        raise HTTPException(status_code=404, detail="Consumer not subscribed or group not found")

    assigned_partitions = [p for p, c in PARTITION_ASSIGNMENTS[group_id].items() if c == consumer_id]
    if not assigned_partitions:
        return Response(status_code=204) # No partitions assigned

    # For simplicity, we'll just try to read from the first assigned partition
    # A real consumer would fetch from all its assigned partitions concurrently
    for partition in assigned_partitions:
        result = storage.read_next_message("my-topic", partition, group_id) # The group_id is the cursor key
        if result:
            message_id, data = result
            return {"partition": partition, "message_id": message_id, "data": data.decode("utf-8")}
    
    return Response(status_code=204) # No new messages in assigned partitions

@app.post("/messages/ack")
def acknowledge(group_id: str, partition: int, message_id: int):
    """Acknowledges a message, updating the group's cursor for a partition."""
    # In this model, the cursor is shared by the group for a partition
    storage.acknowledge_message("my-topic", partition, group_id, message_id)
    return {"status": "ok"}
