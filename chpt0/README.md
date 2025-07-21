
# Mini-Kafka in Python: A Lightweight Messaging System (Part 1)

Building a full-featured distributed pub-sub messaging system like Apache Kafka/Pulsar is a major undertaking, but we can create a simplified "mini" version in Python to illustrate the core ideas.

<!-- This tutorial provides an introduction to the first version of Mini Python Pubsub, a lightweight messaging system inspired by Apache Kafka/Pulsar. We'll cover its core concepts, the design process for this initial version, how to use it, and what to expect in the next iteration. -->

<!-- ## 1. The Basic Architecture

We designed the first version of Mini-Kafka with simplicity as the primary goal, and created a single-node system with the following components:
   1. A FastAPI-based Server (Broker): A web server (`server.py`) that will receive messages from producers, store them, and deliver them to
   consumers. We chose FastAPI framework for the server due to its ease of use and performance.
   2. A Client Library: A simple Python module (`client.py`) that producers and consumers can use to interact with the server over HTTP.
   3. Persistence: A simple file-based storage system (`storage.py`) that handles writing messages to and reading them from the disk.
   <!-- persists messages to disk, with one file per message. 
   4. An example script (`example.py`) demonstrating the producer/consumer workflow. -->

<!-- ## 1. Basic Architecture
### Step 1: Client & Server

The initial goal was to create a simple messaging system with a server (serving as the **broker**) and a client library. We chose FastAPI for the server due to its ease of use and performance.

### Step 2: Simple File-Based Persistence

Our first version of persistence is very straightforward: each message is saved as a separate file. This was easy to implement and demonstrates the basic concept of a persistent log. While this approach has performance limitations (which we will address in the next version), it provides a solid foundation to build upon. -->

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
Mini-Kafka in Python is built around a few key concepts:

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
1.  **The Cursor**: The broker maintains the metadata of all topics. In our implementation, each `topic` object maintains a dictionary called `cursors`, which maps a consumer to the **offset** (list index) that the consumer should read next. In real world implementation, the topics metadata should also be persisted on disk.
2.  **Polling for a Message**: When a consumer calls its `poll` method, it asks the broker for the next message. The broker checks the topic metadata, looks up the consumer's current read offset in the cursors, then fetches the message from the logs at that specific index.
3.  **Acknowledgement (Moving the Cursor)**: After processing a message, the consumer must **acknowledge** it. The consumer calls `consumer.acknowledge(message_id)`, tells the broker to `ack()` the message. The broker then calls the topic to `commit()` the new offset, which simply updates the `cursors` for that consumer, setting its offset value to  the `message_id` it just finished.         

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



## 4. Next Steps

This first version of Mini-Kafka in Python is a great starting point, from which we can understand the core concepts of modern pub/sub systems. 
But there are many ways it could be improved. Here are a few ideas:  
*   **Formal Subscriptions and Consumer Groups**: Currently, consumers are identified by a simple           `consumer_id`. We could introduce a more formal subscription model where multiple consumers can form a  group to process messages in parallel. Each subscription would have its own cursor, allowing different groups to consume the same topic independently.
*   **Message Replication**: For high availability, we could replicate topic data across multiple server instances. This would require a mechanism for leader election and data synchronization.
*   **More Robust API**: The current API is very simple. We could extend it to support features like:
      * Batching messages for higher throughput.
      * Setting message properties or headers.
      * More advanced consumption patterns (e.g., seeking to a specific message ID).
*   **Asynchronous Client**: The current client is synchronous. An asynchronous client (e.g., using `httpx`) would allow for higher performance in applications that need to send or receive many messages concurrently.                                              *   **Configuration**: We could add a configuration file to manage settings like the segment size, data directory, and server port. 

This tutorial provides a solid foundation for understanding and extending MiniPyPubsub.
In the next tutorial, we will introduce a more advanced storage system based on **segmented logs**. This will group messages into larger files (segments) and use indexes to provide fast access, which is a much more scalable approach.
