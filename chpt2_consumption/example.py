
import time
import threading
from mini_kafka.client import KafkaClient

# --- Configuration ---
BASE_URL = "http://127.0.0.1:8000"
TOPIC = "my-topic"
GROUP_ID = "my-consumer-group"

# --- Consumer Worker ---
def run_consumer():
    """A function run by each consumer thread."""
    client = KafkaClient(BASE_URL)
    consumer = client.subscribe(GROUP_ID)
    
    while True:
        msg = consumer.receive()
        if msg:
            print(f"[{client.consumer_id[-5:]}] Received: '{msg['data']}' (key={msg.get('key')}) on partition {msg['partition']}")
            consumer.acknowledge(msg['partition'], msg['message_id'])
        else:
            time.sleep(1)

# --- Main Execution ---
def main():
    # 1. Start consumer threads
    num_consumers = 3
    consumer_threads = []
    for _ in range(num_consumers):
        thread = threading.Thread(target=run_consumer)
        thread.daemon = True
        thread.start()
        consumer_threads.append(thread)

    print(f"Started {num_consumers} consumers in group '{GROUP_ID}'.")
    print("-" * 20)
    time.sleep(5) # Give consumers time to subscribe and the group to balance

    # 2. Create a producer
    client = KafkaClient(BASE_URL)
    producer = client.create_producer(TOPIC)

    # 3. Produce some messages with keys
    print("Producer sending messages with keys...")
    for i in range(5):
        key = f"user-{i % 2}" # Will always go to the same two partitions
        data = f"Event #{i+1} for {key}"
        result = producer.send(data, key=key)
        print(f"Producer sent: '{data}' (key={key}) -> partition {result['partition']}")
        time.sleep(0.5)

    # 4. Produce some messages without keys
    print("\nProducer sending messages without keys...")
    for i in range(3):
        data = f"General event #{i+1}"
        result = producer.send(data)
        print(f"Producer sent: '{data}' (key=None) -> partition {result['partition']}")
        time.sleep(0.5)

    print("-" * 20)
    print("Producer finished. Consumers will continue to run.")
    print("Press Ctrl+C to exit.")

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nExiting...")

if __name__ == "__main__":
    main()
