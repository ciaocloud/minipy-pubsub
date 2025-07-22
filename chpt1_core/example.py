from mini_kafka_v1.client import *
from mini_kafka_v1.server import *
import time


BASE_URL = "http://127.0.0.1:8000"

TOPIC_NAME = "my-topic"
PRODUCER_ID = "my-producer"
CONSUMER_ID = "my-consumer"

# --- Main Execution ---
def main():

    # 1. Initialize the broker
    # broker = Broker()

    # 2. Create a producer and send messages
    # producer = Producer(broker, PRODUCER_ID)
    producer = Producer(BASE_URL, PRODUCER_ID)
    print(f"Sending message to topic '{TOPIC_NAME}'...")
    for i in range(10):
        message_id = producer.send(TOPIC_NAME, f"Hello, Mini Kafka! - message {i}")
        print(f"Message sent with ID: {message_id}")

    # 3. Create a consumer and receive the message
    consumer = Consumer(BASE_URL, CONSUMER_ID, TOPIC_NAME)
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


if __name__ == "__main__":
    main()
