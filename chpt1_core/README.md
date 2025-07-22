
# Mini-Kafka in Python: A Lightweight Messaging System (Part 1)

Building a full-featured distributed pub-sub messaging system like Apache Kafka/Pulsar is a major undertaking, but we can create a simplified "mini" version in Python to illustrate the core ideas.

## 1. Core Concepts
### The Topic: A Stream of Messages
The first concept to grasp is the **topic**. In modern pub/sub systems such as Apache Kafka, a **topic** is a named channel/stream of messages. It's the central point of communication. Producers write messages to a topic, and consumers read messages from it. The topic is a logical concept, and physically it is usually achieved by writing to log files on disk. In the very first version of our implementation, we store the topic in memory for simplicity. Soon we will persist the messages in logs, which is a core functionality of the message broker.
```python
class Topic:
    def __init__(self, name: str):
        self.name = name
        self.messages = list()
        self.write_offset = 0
        self.cursors = dict() ## read offset for each consumer

    def produce(self, message: str):
        self.messages.append(message)
        self.write_offset += 1
        return self.write_offset - 1

    def consume(self, consumer_id: str):
        offset = -1
        if consumer_id in self.cursors:
            offset = self.cursors[consumer_id]
        if offset < 0 or offset >= self.write_offset:
            return None
        return offset, self.messages[offset]

    def commit(self, consumer_id, message_id):
        self.cursors[consumer_id] = message_id
```
In addition to a list of messages to be stored, we also maintain a cursor (called **offset**) for each consumer to record which is the last message that has been received by the consumer, so we know which message in a topic should be read next. This information must be confirmed by the consumer upon successfully processed the message, by sending an **acknowledgement** to the broker, which is also often called **committing the offset** mechanism. This acknowledgement step is crucial. If the consumer crashes before acknowledging, the cursor is not updated; when it restarts, it will receive the same message again, ensuring that no messages are lost. This is a fundamental concept in messaging systems known as **at-least-once delivery**.  

### Core Components: Broker, Producer, and Consumer
Our mini-Kafka in Python follows a classic messaging architecture where messages flow through topics:
```
Producer --> Broker --> Consumer
```

1. **Producer**: A client application that has a `send` method to publish messages to a topic.
```python
class Producer:
    def __init__(self, broker: Broker, producer_id: str):
        self.broker = broker
        self.producer_id = producer_id

    def send(self, topic: str, message: str):
        return self.broker.publish_message(topic, message)
```
2. **Consumer**: A client application that subscribes to a topic to receive messages using the `poll` method, also send `acknowledement` back to the broker.
```python
class Consumer:
    def __init__(self, broker: Broker, consumer_id: str, topic_name: str):
        self.broker = broker
        self.topic_name = topic_name
        self.consumer_id = consumer_id

    def poll(self):
        return self.broker.consume_message(self.topic_name, self.consumer_id)

    def acknowledge(self, message_id):
        self.broker.ack(self.topic_name, self.consumer_id, message_id)
```

In this first version, our producer and consumer would directly call the methods/functions in the broker. In real world application, network protocol (TCP/HTTP) support is needed, which means the broker, as a web server, exposes certain APIs to handle the requests from the client (i.e., the producers and consumers), following the client-server communication paradigm. Later we will use the FastAPI framework to implement this due to its ease of use and performance.

3. **Broker**: The central server hub that receives, stores, and serves messages. 

```python
class Broker:
    def __init__(self):
        self.topics = {}

    def create_topic(self, topic_name):
        if not topic_name in self.topics:
            self.topics[topic_name] = Topic(topic_name)
```
The broker creates, manages and holds **topics**. 

```python
    def publish_message(self, topic_name, message):
        if not topic_name in self.topics:
            self.create_topic(topic_name)
        topic = self.topics[topic_name]
        return topic.produce(message)
```

When a producer sends a message to a topic, it simply tells the broker to publish the message. Upon receiving the request, the broker finds the correct topic and calls its `produce()` method, which appends the message and returns the message's position in the list. This position is its **offset** (or message ID).   

```python
    def consume_message(self, topic_name: str, consumer_id: str):
        topic = self.topics[topic_name]
        result = topic.consume(consumer_id)
        if not result:
            print("No new messages")
            return None
        #     raise Exception("No new messages")
        message_offset, message = result
        return {"message_id": message_offset, "data": message}

    def ack(self, topic_name, consumer_id, message_id):
        topic = self.topics[topic_name]
        topic.commit(consumer_id, message_id)
```
-
    - **The Cursor**: The broker maintains the metadata of all topics. In our implementation, each `topic` object maintains a dictionary called `cursors`, which maps a consumer to the **offset** (list index) that the consumer should read next. In real world implementation, the topics metadata should also be persisted on disk.

    - **Polling for a Message**: When a consumer calls its `poll` method, it asks the broker for the next message. The broker checks the topic metadata, looks up the consumer's current read offset in the cursors, then fetches the message from the logs at that specific index.
    - **Acknowledgement (Moving the Cursor)**: After processing a message, the consumer must **acknowledge** it. The consumer calls `consumer.acknowledge(message_id)`, tells the broker to `ack()` the message. The broker then calls the topic to `commit()` the new offset, which simply updates the `cursors` for that consumer, setting its offset value to  the `message_id` it just finished.         

## 3. Running an Example

```python
TOPIC_NAME = "my-topic"
PRODUCER_ID = "my-producer"
CONSUMER_ID = "my-consumer"

# 1. Initialize the broker
broker = Broker()

# 2. Create a producer and send messages
producer = Producer(broker, PRODUCER_ID)
print(f"Sending message to topic '{TOPIC_NAME}'...")
for i in range(10):
    message_id = producer.send(TOPIC_NAME, f"Hello, Mini Kafka! - message {i}")
    print(f"Message sent with ID: {message_id}")

# 3. Create a consumer and receive the message
consumer = Consumer(broker, CONSUMER_ID, TOPIC_NAME)
print(f"Waiting for message on topic '{TOPIC_NAME}' for consumer '{CONSUMER_ID}'...")

# Poll for the message
i = 0
while True:
    msg = consumer.poll()
    if msg:
        print(f"Received message: {msg['data']} (ID: {msg['message_id']})")

        # 4. Acknowledge the message
        consumer.acknowledge(msg['message_id'])
        print(f"Acknowledged message ID: {msg['message_id']}")
        i += 1
        if i >= 10:
            break
    else:
        print("No new messages. Waiting...")
        time.sleep(5)
```

By running the above example code, you will see output indicating that messages have been sent from the producer side and then received on the consumer side. 
```
Sending message to topic 'my-topic'...
Message sent with ID: 0
Message sent with ID: 1
...
Message sent with ID: 9
Waiting for message on topic 'my-topic' for consumer 'my-consumer'...
Received message: Hello, Mini Kafka! - message 0 (ID: 0)
Acknowledged message ID: 0
...
Received message: Hello, Mini Kafka! - message 9 (ID: 9)
Acknowledged message ID: 9
```

## 4. Message Persistence
The in-memory broker we just implemented is simple and great for illustration of how producers and consumers work with topics, but it has a fatal flaw for any real-world application: if the broker crashes or is restarted, all messages are lost forever.

This is where persistence comes in. Persistence means storing data on a durable medium, such as a hard drive, so that it survive restarts and failures. For a message broker, this provides several key guarantees:
1. Durability: Messages sent to the broker are safe. Once the producer gets a confirmation, it knows the message won't be lost if the broker server reboots.
2. Decoupling: Consumers don't have to be running at the same time as producers. A producer can send messages to a topic, and a consumer can start up hours or days later to process them because the messages have been safely stored.
3. Fault Tolerance: If a consumer processes a message but crashes before it can acknowledge it, the message is not lost. Because the broker has the message stored safely on disk and the consumer's progress (the cursor/offset) is also stored, the consumer can restart, read its last saved position, and re-process the message.

Let's modify our code to implement exactly this, to mimic the log-centric design of Apache Kafka and Pulsar. The `Topic` class in our code handles the storage logic, which serves as the heart of our persistence layer. We can simply make changes to the `Topic` class only, and leave all other components untouched.

```python
from pathlib import Path

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
        # message_file.write_bytes(data)
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
```
As you can see from the code, we are no longer using the in-memory message lists. Instead, we have:
1. Topic as a Directory: Each topic will be a directory on the filesystem. For example, a topic named `orders` will be stored in a folder at a path like `./data/orders/`.
2. Message as a File: Each message sent to a topic will be stored as a separate file within that topic's directory. The files will be named with a sequential, zero-padded integer to represent their order in the log (e.g., `0000000001.msg`, `0000000002.msg`, etc.).
3. Append-Only Log: This structure creates an append-only log. New messages are simply new files added to the end of thesequence. This is fast for writes.
4. Cursor as a File: 
    * The cursor will be a file in the topic directory (e.g., `./data/orders/consumer-a.cursor`) that contains just one piece of information: the ID of the last message that consumer acknowledged.
    * When a consumer requests a message, the server reads its cursor, finds the next message file in the sequence (e.g., if the cursor is 2, it reads `0000000003.msg`), and sends it. After the consumer acknowledges it, the server updates the cursor file to 3. 

By running the same example test code, you will see output indicating that a message has been sent and then received, but also inspect the `data/` directory to see the message files (`.msg`) that have been created.

## 5. Web APIs: Enabling Distributed Communication

The final, crucial step in evolving Mini-Kafka is exposing its functionality through Web APIs. This transforms our in-process broker into a network-accessible service, allowing client applications (producers and consumers) to interact with it over a network, just like with a real Kafka cluster. We use FastAPI to create a simple, clean, and high-performance web server for this purpose. Let's look at the code that defines our API endpoints.

```python
from fastapi import FastAPI, HTTPException

app = FastAPI()

TOPICS = {} # In a real system, this would be persisted

@app.post("/topics/{topic_name}/messages")
def publish_message(topic_name: str, message: str):
    if not topic_name in TOPICS:
        TOPICS[topic_name] = Topic(topic_name)
    topic = TOPICS[topic_name]
    message_offset = topic.produce(message)
    return {"message_id": message_offset}

@app.get("/topics/{topic_name}/messages")
def consume_message(topic_name: str, consumer_id: str):
    topic = TOPICS[topic_name]
    result = topic.consume(consumer_id)
    if not result:
        raise HTTPException(status_code=404, detail="No new messages")
    message_offset, message = result
    return {"message_id": message_offset, "data": message}

@app.post("/topics/{topic_name}/messages/{message_id}/ack")
def ack(topic_name, consumer_id, message_id):
    topic = TOPICS[topic_name]
    topic.commit(consumer_id, message_id)
    return {"status": "ok"}
```

As you can see, we expose the following four endpoints that allow the clients to make HTTP calls and implemented the corresponding handlers, each maps to a method in our previous `Broker` class. 
* `POST /topics/{topic_name}/create`: Create a new topic on the broker.
* `POST /topics/{topic_name}/messages`: Producers send messages to a specific topic. The message content is sent in the request body as JSON.
* `GET /topics/{topic_name}/messages`: Consumers retrieve the next available message for their `consumer_id` from a given topic. The broker returns the message and its ID as a JSON object.
* `POST /topics/{topic_name}/messages/{message_id}/ack`: After successfully processing a message, consumers send this request to update their cursor on the broker, ensuring message delivery guarantees.

On the client side, instead of direct method calling on broker, the producer and consumer now send HTTP requests to the broker's endpoints and receive responses through the network.
```python
class Producer:
    def __init__(self, broker_url: str, producer_id: str):
        self.broker_url = broker_url
        self.producer_id = producer_id

    def send(self, topic_name: str, message: str):
        url = f"{self.broker_url}/topics/{topic_name}/messages"
        response = requests.post(url, params={"message": message})
        return response.json()["message_id"]

class Consumer:
    def __init__(self, broker_url: str, consumer_id: str, topic: str):
        self.url = f"{broker_url}/topics/{topic}/messages"
        self.ack_url_template = f"{broker_url}/topics/{topic}/messages/{{message_id}}/ack"
        self.consumer_id = consumer_id

    def poll(self):
        response = requests.get(self.url, params={"consumer_id": self.consumer_id})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def acknowledge(self, message_id):
        ack_url = self.ack_url_template.format(message_id=message_id)
        response = requests.post(ack_url, params={"consumer_id": self.consumer_id})
        response.raise_for_status()
```

Our API design follows RESTful principles, using standard HTTP methods and clear, resource-oriented URLs. All data exchange, including messages and acknowledgements, is done using **JSON over HTTP**, which has lightweight, human-readable format and is widely adopted for data interchange in web services (later we may also implement **Protobuf** over HTTP to improve the performance). 

Now to run the example test, we need to open two terminals. Let's first start the message broker server one terminal, 
```bash
uvicorn mini_kafka_v0.server:app --host 127.0.0.1 --port 8000
```
The server will now be running and listening for requests.
Then in a separate terminal, run the `example.py` script for the clients (producer and consumer):
```bash
python example.py
```

This architectural shift to web APIs is fundamental for building a truly distributed and scalable messaging system, laying the groundwork for future enhancements like consumer groups and replication.


## 6. Next Steps

This first version of Mini-Kafka in Python is a great starting point, from which we can understand the core concepts of modern pub/sub systems. 
But there are many ways it could be improved. Here are a few ideas:  
*   **Formal Subscriptions and Consumer Groups**: Currently, consumers are identified by a simple `consumer_id`. We could introduce a more formal subscription model where multiple consumers can form a  group to process messages in parallel. Each subscription would have its own cursor, allowing different groups to consume the same topic independently.
*   **Message Replication**: For high availability, we could replicate topic data across multiple server instances. This would require a mechanism for leader election and data synchronization.
*   **More Robust API**: The current API is very simple. We could extend it to support features like batching messages for higher throughput, setting message properties or headers, and more advanced consumption patterns (e.g., seeking to a specific message ID).
*   **Asynchronous Client**: The current client is synchronous. An asynchronous client (e.g., using `httpx`) would allow for higher performance in applications that need to send or receive many messages concurrently. 
*   **Configuration**: We could add a configuration file to manage settings like the segment size, data directory, and server port. 

In the next tutorial, we will introduce and implement **partitions** for parallelism and scalability, **consumer groups** for distributed consumption, **segmented logs and indexes** for efficient storage and retention, etc., transforming our mini-Kafka to be more close to the modern distributed messaging platforms. 
