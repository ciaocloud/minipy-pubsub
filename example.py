
from pulsar_mini.client import PulsarClient
import time

# --- Configuration ---
BASE_URL = "http://127.0.0.1:8000"
TOPIC = "my-topic"
CONSUMER_ID = "my-consumer"

# --- Main Execution ---
def main():
    # 1. Initialize the client
    client = PulsarClient(BASE_URL)

    # 2. Create a producer and send a message
    producer = client.create_producer(TOPIC)
    print(f"Sending message to topic '{TOPIC}'...")
    message_id = producer.send("Hello, Mini Pulsar!")
    print(f"Message sent with ID: {message_id}")

    # 3. Create a consumer and receive the message
    consumer = client.subscribe(TOPIC, CONSUMER_ID)
    print(f"Waiting for message on topic '{TOPIC}' for consumer '{CONSUMER_ID}'...")

    # Poll for the message
    while True:
        msg = consumer.receive()
        if msg:
            print(f"Received message: {msg['data']} (ID: {msg['message_id']})")
            
            # 4. Acknowledge the message
            consumer.acknowledge(msg['message_id'])
            print(f"Acknowledged message ID: {msg['message_id']}")
            break
        else:
            print("No new messages. Waiting...")
            time.sleep(2)

if __name__ == "__main__":
    main()
