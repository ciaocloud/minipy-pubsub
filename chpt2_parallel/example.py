import time
from mini_kafka_v2.client import Producer, Consumer

BROKER_URL = "http://127.0.0.1:8000"
TOPIC_NAME = "my_partitioned_topic"
NUM_PARTITIONS = 5

# 1. Create a topic with multiple partitions
print(f"Creating topic '{TOPIC_NAME}' with {NUM_PARTITIONS} partitions...")
producer_client = Producer(BROKER_URL, "admin_producer")
# The create_topic endpoint is a POST request, but the client doesn't have a direct method for it.
# We'll use requests directly for topic creation for simplicity in this example.
import requests
requests.post(f"{BROKER_URL}/topics/{TOPIC_NAME}/create", params={
    "num_partitions": NUM_PARTITIONS
})
print(f"Topic {TOPIC_NAME} created.")

time.sleep(1) # Give server a moment to set up

# 2. Send messages to different partitions
producer1 = Producer(BROKER_URL, "producer-1")
print(f"Sending message to topic '{TOPIC_NAME}'...")
for i in range(10):
    resp = producer1.send(TOPIC_NAME, f"Hello, Mini Kafka! -- {i}")
    print(resp)
    message_id, partition_id = resp
    print(f"Message sent with ID: {message_id}, to partition {partition_id}")

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
            print(f"  Consumer 1 received from partition {msg['partition_id']}: ID={msg['message_id']}, Data='{msg['data']}'")
            # 4. Acknowledge the message
            consumer1.acknowledge(msg['message_id'], msg['partition_id'])
            message_count_c1 += 1
            print(f"Acknowledged message ID: {msg['message_id']}")
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
            print(f"  Consumer 2 received from partition {msg['partition_id']}: ID={msg['message_id']}, Data='{msg['data']}'")
            # 4. Acknowledge the message
            consumer2.acknowledge(msg['message_id'], msg['partition_id'])
            message_count_c2 += 1
            print(f"Acknowledged message ID: {msg['message_id']}")
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
            print(f"  Consumer 3 received from partition {msg['partition_id']}: ID={msg['message_id']}, Data='{msg['data']}'")
            # 4. Acknowledge the message
            consumer3.acknowledge(msg['message_id'], msg['partition_id'])
            message_count_c3 += 1
            print(f"Acknowledged message ID: {msg['message_id']}")
    else:
        break
print(f"Consumer 3 finished. Received {message_count_c3} messages.")

print("\nExample finished.")
