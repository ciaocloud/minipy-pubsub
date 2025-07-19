
# Mini-Kafka in Python: A Lightweight Messaging System (Part 1)

Building a full-featured distributed pub-sub messaging system like Apache Kafka/Pulsar is a major undertaking, but we can create a simplified "mini" version in Python to illustrate the core ideas.

<!-- This tutorial provides an introduction to the first version of Mini Python Pubsub, a lightweight messaging system inspired by Apache Kafka/Pulsar. We'll cover its core concepts, the design process for this initial version, how to use it, and what to expect in the next iteration. -->

## 1. The Basic Architecture

We designed the first version of Mini-Kafka with simplicity as the primary goal, and created a single-node system with the following components:
   1. A FastAPI-based Server (Broker): A web server (`server.py`) that will receive messages from producers, store them, and deliver them to
   consumers. We chose FastAPI framework for the server due to its ease of use and performance.
   2. A Client Library: A simple Python module (`client.py`) that producers and consumers can use to interact with the server over HTTP.
   3. Persistence: A simple file-based storage system (`storage.py`) that handles writing messages to and reading them from the disk.
   <!-- persists messages to disk, with one file per message. -->
   4. An example script (`example.py`) demonstrating the producer/consumer workflow.

<!-- ## 1. Basic Architecture
### Step 1: Client & Server

The initial goal was to create a simple messaging system with a server (serving as the **broker**) and a client library. We chose FastAPI for the server due to its ease of use and performance.

### Step 2: Simple File-Based Persistence

Our first version of persistence is very straightforward: each message is saved as a separate file. This was easy to implement and demonstrates the basic concept of a persistent log. While this approach has performance limitations (which we will address in the next version), it provides a solid foundation to build upon. -->

## 2. Core Concepts

Mini-Kafka in Python is built around a few key concepts:

*   **Topic**: A named channel for sending and receiving messages.  <!-- Producers send messages to topics, and consumers read messages from topics. -->

*   **Producer**: A client application that sends messages to a specific topic.

*   **Consumer**: A client application that subscribes to a topic to receive messages.

*   **Message**: The unit of data that is sent and received. In our implementation, a message has a unique ID and a payload (the actual data).

*   **Persistence**: Messages will be saved to disk to ensure they are not lost if the server restarts.
We'll use a simple but effective file-system-based approach that mimics the log-centric design of systems like Kafka and Pulsar in the `storage.py` module. 
    *  **Simple File-Based Log**: The storage mechanism used to persist messages. In this first version, each message is stored as a separate file on the filesystem (e.g., `00000001.msg`, `00000002.msg`). This creates a simple, append-only log.
    *   **Cursor**: A pointer that tracks the last message a consumer has successfully processed (acknowledged). This is stored in a `.cursor` file and ensures that a consumer can disconnect and reconnect without losing its place in the message stream.

   <!-- 1. Topic as a Directory: Each topic will be a directory on the filesystem. For example, a topic named orders will be stored in
      a folder at a path like ./data/orders/.

   2. Message as a File: Each message sent to a topic will be stored as a separate file within that topic's directory. The files
      will be named with a sequential, zero-padded integer to represent their order in the log (e.g., `0000000001.msg`,
      `0000000002.msg`, etc.).

   3. Append-Only Log: This structure creates an append-only log. New messages are simply new files added to the end of the
      sequence. This is very fast for writes.


   4. Cursors (Consumer State): To track what messages a consumer has seen, we won't delete messages. Instead, we'll store a
      "cursor" for each consumer subscription.
       * The cursor will be a simple file in the topic directory (e.g., `./data/orders/consumer-a.cursor`).
       * This file will contain just one piece of information: the ID of the last message that consumer acknowledged.
       * When a consumer requests a message, the server reads its cursor, finds the next message file in the sequence (e.g., if
         the cursor is 2, it reads `0000000003.msg`), and sends it. After the consumer acknowledges it, the server updates the
         cursor file to 3. -->


  This persistency design provides:
   * Durability: Messages are written to disk and will survive a server restart.
   * Simplicity: It's easy to implement and understand using basic file I/O.
   * Replayability: Since messages aren't deleted, a new consumer can join and read all the messages from the very beginning.

 The `storage.py` module will have functions like `write_message(topic, data)` and `read_next_message(topic, consumer_id)`.


## 3. How to Use Mini Kafka

Here's how to get Mini Kafka up and running:

### Prerequisites

*   Python 3.7+
*   `uv` (or `pip`)

### Installation

1.  **Clone the repository** (or use the files we've created).

2.  **Install the dependencies**:

    ```bash
    uv add -r requirements.txt
    ```

### Running the System

1.  **Start the message broker server**:

    ```bash
    uvicorn mini_kafka.server:app --host 127.0.0.1 --port 8000
    ```

    The server will now be running and listening for requests.

2.  **Run the example producer and consumer**:

    In a separate terminal, run the `example.py` script:

    ```bash
    python example.py
    ```

    You will see output indicating that a message has been sent and then received. You can also inspect the `data/` directory to see the message files (`.msg`) that have been created.

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
