from pulsar_mini.client import PulsarClient
import time
import threading

# --- Configuration ---
BASE_URL = "http://127.0.0.1:8000"
TOPIC = "my-topic" # This topic has 4 partitions (see server.py)

# --- Consumer Worker ---
def run_consumer(partition: int):
    """A function run by each consumer thread."""
    consumer_id = f"consumer-p{partition}"
    print(f"[{consumer_id}] Starting...")
    client = PulsarClient(BASE_URL)
    consumer = client.subscribe(TOPIC, partition, consumer_id)
    
    while True:
        msg = consumer.receive()
        if msg:
            print(f"[{consumer_id}] Received message: '{msg['data']}' (ID: {msg['message_id']}) on partition {partition}")
            consumer.acknowledge(msg['message_id'])
            print(f"[{consumer_id}] Acknowledged message ID: {msg['message_id']}")
        else:
            # print(f"[{consumer_id}] No new messages.")
            time.sleep(1)

# --- Main Execution ---
def main():
    # 1. Initialize the client and create a producer
    client = PulsarClient(BASE_URL)
    producer = client.create_producer(TOPIC)

    # 2. Start consumer threads, one for each partition
    num_partitions = 4 # Should match the server config
    consumer_threads = []
    for i in range(num_partitions):
        thread = threading.Thread(target=run_consumer, args=(i,))
        thread.daemon = True # Threads will exit when the main program exits
        thread.start()
        consumer_threads.append(thread)

    print(f"Started {num_partitions} consumers.")
    print("-" * 20)
    time.sleep(2) # Give consumers a moment to start up

    # 3. Produce some messages
    # The producer will automatically send them to different partitions in a round-robin fashion
    print("Producer sending 8 messages...")
    for i in range(8):
        message_text = f"Message #{i+1}"
        producer.send(message_text)
        print(f"Producer sent: '{message_text}'")
        time.sleep(0.5)
    
    print("-" * 20)
    print("Producer finished. Consumers will continue to run.")
    print("Press Ctrl+C to exit.")

    # Keep the main thread alive to let consumers run
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nExiting...")

if __name__ == "__main__":
    main()