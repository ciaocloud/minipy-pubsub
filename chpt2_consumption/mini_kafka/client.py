
import requests
import uuid

class KafkaClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.consumer_id = str(uuid.uuid4())

    def create_producer(self, topic: str) -> 'Producer':
        return Producer(self.base_url, topic)

    def subscribe(self, group_id: str) -> 'Consumer':
        """Subscribes this client to a consumer group."""
        url = f"{self.base_url}/subscriptions/subscribe"
        response = requests.post(url, json={"consumer_id": self.consumer_id, "group_id": group_id})
        response.raise_for_status()
        print(f"Consumer '{self.consumer_id}' subscribed to group '{group_id}'. Assignments: {response.json()['assignments']}")
        return Consumer(self.base_url, group_id, self.consumer_id)

class Producer:
    def __init__(self, base_url: str, topic: str):
        self.url = f"{base_url}/topics/{topic}/messages"

    def send(self, data: str, key: str | None = None):
        """Sends a message, optionally with a key."""
        response = requests.post(self.url, json={"key": key, "data": data})
        response.raise_for_status()
        return response.json()

class Consumer:
    def __init__(self, base_url: str, group_id: str, consumer_id: str):
        self.base_url = base_url
        self.group_id = group_id
        self.consumer_id = consumer_id
        self.consume_url = f"{self.base_url}/messages"
        self.ack_url = f"{self.base_url}/messages/ack"

    def receive(self) -> dict | None:
        """Receives a message from one of the partitions assigned to this consumer."""
        params = {"group_id": self.group_id, "consumer_id": self.consumer_id}
        response = requests.get(self.consume_url, params=params)
        if response.status_code == 204:
            return None
        response.raise_for_status()
        return response.json()

    def acknowledge(self, partition: int, message_id: int):
        """Acknowledges a message on a specific partition for the group."""
        params = {"group_id": self.group_id, "partition": partition, "message_id": message_id}
        response = requests.post(self.ack_url, params=params)
        response.raise_for_status()
