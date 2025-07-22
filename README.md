
# Mini Pulsar: A Lightweight Messaging System

This document provides an introduction to Mini Pulsar, a lightweight messaging system inspired by Apache Pulsar. We'll cover its core concepts, the design process we followed, how to use it, and potential future improvements.

## 1. Core Concepts

Mini Pulsar is built around a few key concepts:

*   **Topic**: A named channel for sending and receiving messages. Producers send messages to topics, and consumers read messages from topics.

*   **Producer**: A client application that sends messages to a specific topic.

*   **Consumer**: A client application that subscribes to a topic to receive messages.

*   **Message**: The unit of data that is sent and received. In our implementation, a message has a unique ID and a payload (the actual data).

*   **Segmented Log**: The storage mechanism used to persist messages. Instead of storing each message as a separate file, messages are grouped into *segments*. Each segment is a log file (`.log`) containing multiple messages. This approach is efficient for both writing and reading messages.

*   **Index**: To avoid scanning an entire segment file to find a message, each segment has a corresponding index file (`.index`). The index maps a message ID to its exact position (byte offset) in the segment file, allowing for very fast lookups.

*   **Cursor**: A pointer that tracks the last message a consumer has successfully processed (acknowledged). This ensures that a consumer can disconnect and reconnect without losing its place in the message stream.

### Architecture Overview

The Mini Pulsar system follows a classic messaging architecture:

```
+-----------+     +--------+     +-----------+
| Producer  | --> | Broker | --> | Consumer  |
+-----------+     +--------+     +-----------+
      ^                 |                ^
      |                 |                |
      +-----------------+----------------+
           Messages flow through Topics
```

- **Producer**: Sends messages to a specific topic on the Broker.
- **Broker**: Receives messages from Producers, stores them in segmented logs, and delivers them to subscribed Consumers.
- **Consumer**: Subscribes to a topic on the Broker and receives messages.

## 2. The Design Process

We designed Mini Pulsar iteratively, starting with a simple implementation and gradually adding more sophisticated features.

### Step 1: The Basic Idea

The initial goal was to create a simple messaging system with a central server and a client library. We chose FastAPI for the server due to its ease of use and performance.

### Step 2: Simple File-Based Persistence

Our first version of persistence was very straightforward: each message was saved as a separate file (e.g., `00000001.msg`, `00000002.msg`). This was easy to implement but had a major drawback: it would create a very large number of small files, which is inefficient for most filesystems.

### Step 3: Introducing Segments and Indexes

To address the limitations of the simple file-based approach, we refactored the storage layer to use a segmented log architecture. This is the current implementation and is a significant improvement:

1.  **Message Abstraction**: We defined a clear structure for messages, including metadata like length, which is crucial for reading a stream of messages from a single file.

2.  **Segments**: We introduced segment files to group messages together. This reduces the number of files on the filesystem and improves performance.

3.  **Indexes**: We added index files to allow for fast message lookups within segments. This is a key feature for making the system scalable.

This iterative process allowed us to build a more robust and performant system while keeping the complexity manageable at each step.

## 3. How to Use Mini Pulsar

Here's how to get Mini Pulsar up and running:

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
    uvicorn pulsar_mini.server:app --host 127.0.0.1 --port 8000
    ```

    The server will now be running and listening for requests.

2.  **Run the example producer and consumer**:

    In a separate terminal, run the `example.py` script:

    ```bash
    python example.py
    ```

    You will see output indicating that a message has been sent and then received. You can also inspect the `data/` directory to see the segment and index files that have been created.

## 4. Next Step Improvements

Mini Pulsar is a great starting point, but there are many ways it could be improved. Here are a few ideas:

*   **Formal Subscriptions and Consumer Groups**: Currently, consumers are identified by a simple `consumer_id`. We could introduce a more formal subscription model where multiple consumers can form a group to process messages in parallel. Each subscription would have its own cursor, allowing different groups to consume the same topic independently.

*   **Message Replication**: For high availability, we could replicate topic data across multiple server instances. This would require a mechanism for leader election and data synchronization.

*   **More Robust API**: The current API is very simple. We could extend it to support features like:
    *   Batching messages for higher throughput.
    *   Setting message properties or headers.
    *   More advanced consumption patterns (e.g., seeking to a specific message ID).

*   **Asynchronous Client**: The current client is synchronous. An asynchronous client (e.g., using `httpx`) would allow for higher performance in applications that need to send or receive many messages concurrently.

*   **Configuration**: We could add a configuration file to manage settings like the segment size, data directory, and server port.

This tutorial provides a solid foundation for understanding and extending Mini Pulsar. Feel free to experiment with the code and try implementing some of these improvements!
