from pathlib import Path
from fastapi import FastAPI, HTTPException

class Partition:
    def __init__(self, topic_name, partition_id, data_dir: str = "data"):
        self.topic_name = topic_name
        self.partition_id = partition_id
        self.partition_dir = Path(data_dir) / topic_name / f"partition_{partition_id}"
        self.partition_dir.mkdir(parents=True, exist_ok=True)

    def get_cursor_file(self, consumer_id: str) -> Path:
        return self.partition_dir / f"{consumer_id}.cursor"

    def produce(self, message: str) -> int:
        """Writes a message to a topic's log and returns the message ID."""
        # First find the next message ID
        message_files = sorted(self.partition_dir.glob("*.msg"))
        next_id = 1
        if message_files:
            last_id = int(message_files[-1].stem)
            next_id = last_id + 1

        message_file = self.partition_dir / f"{next_id:08d}.msg"
        message_file.write_text(message)
        return next_id

    def consume(self, consumer_id: str):
        """Reads the next message for a consumer, based on their cursor."""
        cursor_file = self.get_cursor_file(consumer_id)
        last_read_id = 0
        if cursor_file.exists():
            last_read_id = int(cursor_file.read_text())

        message_files = sorted(self.partition_dir.glob("*.msg"))
        for message_file in message_files:
            message_id = int(message_file.stem)
            if message_id > last_read_id:
                return message_id, message_file.read_bytes()
        return None

    def commit(self, consumer_id: str, message_id: int):
        """Updates the consumer's cursor to the given message ID."""
        cursor_file = self.get_cursor_file(consumer_id)
        cursor_file.write_text(str(message_id))

class Topic:
    def __init__(self, name: str, num_partitions: int = 1):
        self.name = name
        self.num_partitions = num_partitions
        self.partitions = []
        for i in range(self.num_partitions):
            partition = Partition(self.name, i)
            self.partitions.append(partition)
        self._round_robin_counter = 0

    def get_partition(self, key: str | None):
        ## Partitioning strategy. Note in Apache Kafka, a partitioner is in instantiated in producer.
        if not key:
            ## simple round-robin
            partition_id = self._round_robin_counter % self.num_partitions
            self._round_robin_counter += 1
        else:
            ## simple hash by key
            partition_id = hash(key) % self.num_partitions
        return partition_id, self.partitions[partition_id]


class ConsumerGroup:
    def __init__(self, group_id: str, topic: Topic):
        self.topic = topic
        self.group_id = group_id
        self.assignments: dict[str, list[int]] = {} ## format: {consumer_id: [partitions]}

    def register_consumer(self, consumer_id: str):
        if consumer_id not in self.assignments:
            self.assignments[consumer_id] = []
        self.rebalance()

    def get_assignment(self, consumer_id: str):
        return self.assignments.get(consumer_id, [])

    def rebalance(self):
        consumers_in_group = list(self.assignments.keys())
        for consumer in consumers_in_group:
            self.assignments[consumer] = []
        for partition_id in range(self.topic.num_partitions):
            i = partition_id % len(consumers_in_group)
            consumer = consumers_in_group[i]
            self.assignments[consumer].append(partition_id)
        print("[BROKER] Rebalance Complete:")
        for consumer, partitions in self.assignments.items():
            print(f"  - Consumer '{consumer}' assigned partitions: {partitions}")

app = FastAPI()

TOPICS: dict[str, Topic] = {}
CONSUMER_GROUPS: dict[str, ConsumerGroup] = {}
# CONSUMER_GROUPS: dict[str, dict[str, list[int]]] = {} #Format: {group_id: {consumer_id: [partitions]}}

@app.post("/topics/{topic_name}/create")
def create_topic(topic_name, num_partitions: int = 1):
    if not topic_name in TOPICS:
        topic = Topic(topic_name, num_partitions)
        TOPICS[topic_name] = topic

@app.post("/topics/{topic_name}/groups/{group_id}/subscribe")
def subscribe_consumer(topic_name: str, group_id: str, consumer_id: str):
    if not topic_name in TOPICS:
        raise HTTPException(status_code=404, detail="Topic not found")
    topic = TOPICS[topic_name]
    if group_id not in CONSUMER_GROUPS:
        CONSUMER_GROUPS[group_id] = ConsumerGroup(topic_name, topic)
    consumer_group = CONSUMER_GROUPS[group_id]
    consumer_group.register_consumer(consumer_id)
    return {"assigned_partitions": consumer_group.get_assignment(consumer_id)}

@app.post("/topics/{topic_name}/messages")
def publish_message(topic_name: str, message: str, key: str):
    """Receives a message from a producer and writes it to the topic's log."""
    if not topic_name in TOPICS:
        create_topic(topic_name)
    topic = TOPICS[topic_name]
    partition_id, partition = topic.get_partition(key)
    message_offset = partition.produce(message)
    return {"message_offset": message_offset, "partition_id": partition_id}

@app.get("/topics/{topic_name}/messages")
def consume_messages(topic_name: str, consumer_id: str, group_id: str):
    """Retrieves the next message for a consumer from a specific partition."""
    if not topic_name in TOPICS:
        raise HTTPException(status_code=404, detail="Topic not found")
    topic = TOPICS[topic_name]
    if group_id not in CONSUMER_GROUPS:
        CONSUMER_GROUPS[group_id] = ConsumerGroup(topic_name, topic)
    consumer_group = CONSUMER_GROUPS[group_id]
    if not consumer_id in consumer_group.assignments:
        consumer_group.register_consumer(consumer_id)
    # if not group_id in CONSUMER_GROUPS:
    #     CONSUMER_GROUPS[group_id] = {}
    # CONSUMER_GROUPS[group_id][consumer_id] = list(range(topic.num_partitions))
    # partition_ids = CONSUMER_GROUPS[group_id][consumer_id] #Format: {group_id: {consumer_id: [partitions]}}
    partition_ids = consumer_group.get_assignment(consumer_id)
    if not partition_ids:
        raise HTTPException(status_code=404, detail="Consumer not assigned to any partitions.")

    messages = []
    for partition_id in partition_ids:
        result = topic.partitions[partition_id].consume(consumer_id)
        if not result:
            continue
            # raise HTTPException(status_code=404, detail="No new messages")
        message_offset, message = result
        messages.append({"message_offset": message_offset, "data": message, "partition_id": partition_id})
    return messages

@app.post("/topics/{topic_name}/messages/{message_offset}/ack")
def ack(topic_name: str, consumer_id: str, partition_id: int, message_offset: int):
    """Acknowledges a message, updating the consumer's cursor."""
    partition = TOPICS[topic_name].partitions[partition_id]
    partition.commit(consumer_id, message_offset)
    return {"status": "ok"}
