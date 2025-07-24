# Mini-Kafka in Python: Part 2 - Partitions, Consumer Groups, and Scalability

This tutorial explains how to leverage partitions, consumer groups, and parallel producers/consumers.
We will delve into these critical features that enable parallelism, flexible consumption, bringing our system closer to the architecture of a scalable and fault-tolerant distributed messaging platform like Apache Kafka.

## 1. Motivation: The Quest for Parallelism and Flexible Consumption

Our `mini_kafka_v1` implementation successfully demonstrated the core producer-broker-consumer interaction and basic persistence. However, it had significant limitations that would hinder its use in any practical, high-throughput scenario. Its **lack of parallelism** (i.e., a single producer and consumer)  often becomes a bottleneck, limiting the overall throughput of the system. Furthermore, different applications may need to consume the same stream of data for various purposes, each requiring independent progress tracking.

This is where the concepts of **partitions** and **consumer groups** become indispensable. 
<!-- They address the fundamental challenges of:
1.  **Scalability:** How can we handle an ever-increasing volume of messages without overwhelming a single processing unit?
2.  **Throughput:** How can we maximize the rate at which messages are produced and consumed?
3.  **Fault Tolerance:** What happens if a consumer fails? How can we ensure that message processing continues seamlessly?
4.  **Flexible Consumption:** How can multiple applications or instances of the same application consume the same data stream independently, without interfering with each other's progress? -->

At its heart, we are transforming our topic from a single log into a **set of parallel logs (partitions)**. This allows us to get the best of both worlds:
*   **Queue Behavior (Load Balancing):** By introducing partitions, we can distribute messages across multiple physical or logical units to share the load, enabling parallel processing. This is the classic behavior of a message queue.
*   **Pub/Sub Behavior (Broadcast):** We also want different applications (or "subscriber groups") to be able to consume the same stream of data independently, without interfering with each other. For example, a real-time analytics service and an archiving service should both be able to read all the messages from a topic. Consumer groups, provide a mechanism for coordinating multiple consumers to work together on a shared data stream, ensuring both load balancing and fault tolerance. 

This tutorial will walk you through how these concepts are implemented and utilized in our Mini-Kafka.

## 2. Core Concepts

### 2.1. Partitions: From Single Log to Parallel Queues

In our first version of implementation (`mini_kafka_v1`), a topic was a single, monolithic log. In `mini_kafka_v2`, a topic is now divided into multiple **partitions**. Each partition is an independent, ordered, and immutable sequence of messages. This design has profound implications:

*   **Scalability:** By distributing a topic's data across multiple partitions, the system can handle a much larger volume of messages. Each partition can reside on a different server (though in our mini-version, they are logical divisions within a single broker), allowing for horizontal scaling.
*   **Parallelism:** Both producers and consumers can interact with partitions in parallel. Producers can write to different partitions concurrently, this also gives us control over data distribution; and consumers can read from different partitions simultaneously, significantly increasing throughput.
*   **Ordering Guarantees:** While there's no global ordering across an entire topic, messages *within a single partition* are strictly ordered. This is crucial for applications that require sequential processing of related events.


In Apache Kafka, the number of partitions (for each topic) is specified during the topic creation. Note that over time, increasing the number of partitions for an existing topic is generally safe, actually this is a common operation to scale out a topic's capacity or to add more parallelism for consumers. However, decreasing the number of partitions is generally NOT supported or high risky. We will discuss this more when we focus on the storage replication.

Once the partitions are created, the partitioning (i.e., which partition should a message be sent and stored into) is primarily determined by the producer. In Apache Kafka, there is a `Partitioner` (which is part of the `Producer` client library) lives inside the producer that allows us to implement **partitioning strategies** and decides the target partition ID. 

- **Explicit**: If the producer specifies a partition, the message will be routed there directly. Note this requires the producer keeps track of all partitions.
- **Hash-based**: Usually the producer provides a (optional) `MessageKey` that represents a logical grouping for the message, for instance, we can use `customer_id` or `order_id` to be the message key in an e-commerce system. The partitioner could then hash the key and use that to determine the target partition, e.g., `partition_id = hash(MessageKey) % num_partitions`. This ensures that all messages with the same key will always go to the same partition, also is crucial for maintaining message order for a given key, since they all land in the same partition in the order they were sent.
- **Round-Robin**: If no key is provided, other partitioning strategy would be applied, and the default behavior is typically round-robin, i.e., the producer will distribute messages sequentially across all available partitions, this ensures an even distribution of messages when ordering by key is not important.
- **Customized**: You may even implement your own partitioning strategy (e.g., range-based, or add business logic) by implement a custom `Partitioner` in Apache Kafka. 

In our mini-Kafka implementation, we implemented the simple hash by key and round-robin strategies. Note that unlike Apache Kafka, we put the partitioning logic inside the broker (as in the `Topic` class) for simplicity purpose, so that our producer does not need to maintain the topics and partitions information, which is informed by the broker thus may have inconsistency issue. Our producer simply adds an (optional) `key` while sending messages.

**Producer (`mini_kafka_v2/client.py`) **

```python
class Producer:
    # ...    
    def send(self, topic_name: str, message: str, key: str = ""):
        url = f"{self.broker_url}/topics/{topic_name}/messages"
        response = requests.post(url, params={"message": message, "key": key})
        return response.json()
```

On the broker side, we refactored the `Topic` class with the initialization of the partitions at topic creation time. 
The `Topic` now contains a list of (pointers to) the `Partition` objects, and the `get_partition` method determines which partition a new message should be written to.
Now it is the `Partition` role to perform the message storage, managing its own dedicated directory for read and write. Thus we moved the `produce`, `consume`, `commit` methods from the `Topic` class to `Partition`, with almostly identical operations as before but on the partition's directories.

**Broker (`chpt2_parallel/mini_kafka_v2/server.py` - `Topic` and `Partition` classes):**

```python
class Partition:
    def __init__(self, topic_name, partition_id, data_dir: str = "data"):
        self.topic_name = topic_name
        self.partition_id = partition_id
        self.partition_dir = Path(data_dir) / topic_name / f"partition_{partition_id}"
        self.partition_dir.mkdir(parents=True, exist_ok=True)

    # ... (produce, consume, commit methods operate on this partition_dir)
    def get_cursor_file(self, consumer_id: str) -> Path:
        ...
    def produce(self, message: str) -> int:
        ...
    def consume(self, consumer_id: str):
        ...
    def commit(self, consumer_id: str, message_id: int):
        ...

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
```


### 2.2. Consumer Groups: The Unit of Subscription

Consumer groups are a powerful abstraction that allows multiple consumers to work together to consume messages from a topic. All consumers that connect with the same `group_id` are considered a single logical subscriber. 

- The golden rule is that within a consumer group, the system (as the logic in our `rebalance` method) will guarantee that each partition is assigned to **exactly one** consumer from the group at any given time. 
- This ensures that messages from a partition are processed by only one consumer in the same group, preventing duplicate processing while distributing the workload across multiple consumer instances. 
- Note this also limits the maximum parallelism of a consumer group to be equal the number of partitions in the topic, e.g., if you have 5 partitions, you can have at most 5 active consumers in a group. Any additional consumers will be idle.
- If one consumer crashes, the system will automatically re-assign its partition to one of the remaining consumers. This provides both **scalability** and **fault tolerance**.
- More importantly, it enables **Pub/Sub**: different consumer groups can consume the same topic independently, each maintaining its own committed offsets, without affecting each other.
If a new application needs to read the same data, it simply starts consuming with a *different* `group_id`. The system will treat it as a completely separate subscriber, managing its offsets independently and giving it its own view of the topic.

> Personally, I think the design of Apache Pulsar's subscription-centric consumption is better than Kafka's partition-centric consumption, provides significantly more flexibility and is more modern. I might implement a mini-version of that along with an article, comparing the two architecture.

**`ConsumerGroup` class (`chpt2_parallel/mini_kafka_v2/server.py`):**

```python
class ConsumerGroup:
    def __init__(self, group_id: str, topic: Topic):
        self.topic = topic
        self.group_id = group_id
        self.assignments: dict[str, list[int]] = {} ## format: {consumer_id: [partitions]}

    def register_consumer(self, consumer_id: str):
        if consumer_id not in self.assignments:
            self.assignments[consumer_id] = []
        self.rebalance() # Assign partitions to consumers in the group

    def rebalance(self):
        """ distribute partitions among registered consumers """
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
```

The `ConsumerGroup` class manages which consumers are part of a group and which partitions they are assigned to. 

In broker, in addition to the topics metadata, we now also need to maintain the metadata for partitions, as well as their assignments for all the subscribed consumers.

The new `/subscribe` endpoint allows consumers to explicitly join a group, triggering a rebalance. The `consume_messages` endpoint now uses the consumer group and its assignments to fetch messages.

**Broker API endpoints and Handlers (`chpt2_parallel/mini_kafka_v2/server.py`)**
```python
app = FastAPI()

TOPICS: dict[str, Topic] = {}
CONSUMER_GROUPS: dict[str, ConsumerGroup] = {} ## {group_id: {consumer_id: [partitions]}}

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
```

We make the corresponding changes on the client side for the consumer, now has a new method to call the server's new `/subscribe` API endpoint to join a consumer group (with `group_id`), and stores the assigned partitions.
The consumers now `poll` with `consumer_id` and `group_id`, and the acknowledgement includes the `partition_id`. 

**`mini_kafka_v2/client.py` (Consumer):**
```python
class Consumer:
    def __init__(self, broker_url: str, consumer_id: str, topic: str, group_id: str):
        self.broker_url = broker_url
        self.url = f"{broker_url}/topics/{topic}/messages"
        self.ack_url_template = f"{broker_url}/topics/{topic}/messages/{{message_offset}}/ack"
        self.consumer_id = consumer_id
        self.group_id = group_id
        self.subscribe(topic, group_id)

    def subscribe(self, topic, group_id):
        subscribe_url = f"{self.broker_url}/topics/{topic}/groups/{group_id}/subscribe"
        response = requests.post(subscribe_url, params={"consumer_id": self.consumer_id})
        response.raise_for_status()
        return response.json()["assigned_partitions"]

    def poll(self):
        response = requests.get(self.url, params={"consumer_id": self.consumer_id, "group_id": self.group_id})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def acknowledge(self, message_offset, partition_id):
        ack_url = self.ack_url_template.format(message_offset=message_offset)
        response = requests.post(ack_url, params={"consumer_id": self.consumer_id, "partition_id": partition_id})
        response.raise_for_status()
```

## 3. Running an Example
```python
BROKER_URL = "http://127.0.0.1:8000"
TOPIC_NAME = "my_partitioned_topic"
NUM_PARTITIONS = 3

# 1. Create a topic with multiple partitions
print(f"Creating topic '{TOPIC_NAME}' with {NUM_PARTITIONS} partitions...")
producer_client = Producer(BROKER_URL, "admin_producer")
# The create_topic endpoint is a POST request, but a direct method for it locates in Admin in Apache Kafka.
# We'll directly make HTTP request for topic creation for simplicity in this example.
import requests
requests.post(f"{BROKER_URL}/topics/{TOPIC_NAME}/create", params={
    "num_partitions": NUM_PARTITIONS
})
print(f"Topic {TOPIC_NAME} created.")

time.sleep(1) # Give server a moment to set up

# 2. Send messages to different partitions
producer1 = Producer(BROKER_URL, "producer-1")
print(f"Sending message to topic '{TOPIC_NAME}'...")
for i in range(60):
    resp = producer1.send(TOPIC_NAME, f"Hello, Mini Kafka! -- {i}")
    message_offset, partition_id = resp["message_offset"], resp["partition_id"]
    print(f"Message sent with offset: {message_offset}, to partition {partition_id}")

time.sleep(1) # Give server a moment to process messages

# 3. Create consumers belonging to a consumer group and consuming from specific partitions
print("\nCreating consumers and polling for messages...")
GROUP_ID = "my_consumer_group"

# Consumer 1: Consumes from partition 0
consumer1 = Consumer(BROKER_URL, "consumer-1", TOPIC_NAME, GROUP_ID)
print(f"Consumer 1 (group: {GROUP_ID}, id: consumer-1) polling partition 0...")
message_count_c1 = 0
while True:
    msgs = consumer1.poll()
    if msgs:
        for msg in msgs:
            print(f"  Consumer 1 received from partition {msg['partition_id']}: ID={msg['message_offset']}, Data='{msg['data']}'")
            # 4. Acknowledge the message
            consumer1.acknowledge(msg['message_offset'], msg['partition_id'])
            message_count_c1 += 1
            print(f"Acknowledged message ID: {msg['message_offset']}")
    else:
        break
print(f"Consumer 1 finished. Received {message_count_c1} messages.")

# Consumer 2: Consumes from partition 1
consumer2 = Consumer(BROKER_URL, "consumer-2", TOPIC_NAME, GROUP_ID)
print(f"Consumer 2 (group: {GROUP_ID}, id: consumer-2) polling partition 1...")
message_count_c2 = 0
while True:
    msgs = consumer2.poll()
    if msgs:
        for msg in msgs:
            print(f"  Consumer 2 received from partition {msg['partition_id']}: ID={msg['message_offset']}, Data='{msg['data']}'")
            # 4. Acknowledge the message
            consumer2.acknowledge(msg['message_offset'], msg['partition_id'])
            message_count_c2 += 1
            print(f"Acknowledged message ID: {msg['message_offset']}")
    else:
        break
print(f"Consumer 2 finished. Received {message_count_c2} messages.")

# Consumer 3: Consumes from partition 2
consumer3 = Consumer(BROKER_URL, "consumer-3", TOPIC_NAME, GROUP_ID)
print(f"Consumer 3 (group: {GROUP_ID}, id: consumer-3) polling partition 2...")
message_count_c3 = 0
while True:
    msgs = consumer3.poll()
    if msgs:
        for msg in msgs:
            print(f"  Consumer 3 received from partition {msg['partition_id']}: ID={msg['message_offset']}, Data='{msg['data']}'")
            # 4. Acknowledge the message
            consumer3.acknowledge(msg['message_offset'], msg['partition_id'])
            message_count_c3 += 1
            print(f"Acknowledged message ID: {msg['message_offset']}")
    else:
        break
print(f"Consumer 3 finished. Received {message_count_c3} messages.")

print("\nExample finished.")

```

Similar to the example we ran for `mini_kafka_v1` in the first tutorial, we need to start the message broker server in one terminal first with
```bash
uvicorn mini_kafka_v0.server:app --host 127.0.0.1 --port 8000
```
Then by running the above example code in a separate terminal for the clients (producer and consumer), you will see output indicating that a message has been routed to different partitions in a round-robin fashion, and then received by different consumers. Also inspect the `data/` directory to see the message files (`.msg`) and consumer cursors (`.cursor`) that have been created in different partition directories. 
```
Creating topic 'my_partitioned_topic' with 6 partitions...
Topic my_partitioned_topic created.
Sending message to topic 'my_partitioned_topic'...
Message sent with offset: 1, to partition 0
Message sent with offset: 1, to partition 1
Message sent with offset: 1, to partition 2
Message sent with offset: 1, to partition 3
Message sent with offset: 1, to partition 4
Message sent with offset: 1, to partition 5
Message sent with offset: 2, to partition 0
...
```

On the server-side terminal, you will see logs showing the consumer group rebalancing as each new consumer joins:
```
[BROKER] Rebalance Complete:
  - Consumer 'consumer-1' assigned partitions: [0, 1, 2, 3, 4, 5]

...

[BROKER] Rebalance Complete:
  - Consumer 'consumer-1' assigned partitions: [0, 2, 4]
  - Consumer 'consumer-2' assigned partitions: [1, 3, 5]

...

[BROKER] Rebalance Complete:
  - Consumer 'consumer-1' assigned partitions: [0, 3]
  - Consumer 'consumer-2' assigned partitions: [1, 4]
  - Consumer 'consumer-3' assigned partitions: [2, 5]
```

## 4. Conclusion and Next Steps: Implementing Replication for Fault Tolerance

`mini_kafka_v2` represents a significant leap forward from its predecessor. By implementing **partitions** and **consumer groups**, we have transformed a basic messaging system into one capable of handling higher throughput, providing fault tolerance, and supporting flexible consumption patterns. This version lays a robust foundation for building more complex and resilient distributed applications, mirroring the core capabilities of industry-standard messaging platforms.

The most critical missing piece in our system is fault tolerance. Currently, if our single broker instance fails, all data is lost and the system goes offline. The next tutorial will address this by implementing a replication mechanism, evolving our project into `mini_kafka_v3`.
The key concepts we will introduce are:

*   **Broker Cluster:** We will run multiple broker instances to form a cluster.
*   **Coordinator:** We will need a central controller responsible for managing cluster metadata, tracking broker health, and orchestrating leader elections. In real Kafka deployment, this role is often played by ZooKeeper or KRaft.
*   **Leader and Follower Replicas:** For each partition, one replica will be the designated **leader** (handling all reads and writes), while others will be **followers** that synchronize their data from the leader.
*   **In-Sync Replicas (ISR):** A message will only be considered committed when it has been successfully replicated to a set of in-sync followers, guaranteeing no data loss if the leader fails.

This will bring our Mini-Kafka implementation much closer to the resilient architecture of real-world distributed messaging systems.
