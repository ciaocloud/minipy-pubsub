import requests
from itertools import cycle

class PulsarClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def create_producer(self, topic: str) -> 'Producer':
        """Creates a producer for a given topic."""
        # Get the number of partitions for the topic
        response = requests.get(f"{self.base_url}/topics/{topic}")
        response.raise_for_status()
        num_partitions = response.json()["partitions"]
        return Producer(self.base_url, topic, num_partitions)

    def subscribe(self, topic: str, partition: int, consumer_id: str) -> 'Consumer':
        """Subscribes to a specific partition of a topic."""
        return Consumer(self.base_url, topic, partition, consumer_id)

class Producer:
    def __init__(self, base_url: str, topic: str, num_partitions: int):
        self.base_url = base_url
        self.topic = topic
        self.num_partitions = num_partitions
        # Create a cyclical iterator for round-robin partition selection
        self._partition_cycler = cycle(range(num_partitions))

    def send(self, data: str):
        """Sends a message to a partition in a round-robin fashion."""
        partition = next(self._partition_cycler)
        url = f"{self.base_url}/topics/{self.topic}/{partition}/messages"
        response = requests.post(url, json={"data": data})
        response.raise_for_status()
        return response.json()["message_id"]

class Consumer:
    def __init__(self, base_url: str, topic: str, partition: int, consumer_id: str):
        self.url = f"{base_url}/topics/{topic}/{partition}/messages"
        self.ack_url_template = f"{base_url}/topics/{topic}/{partition}/messages/{{message_id}}/ack"
        self.consumer_id = consumer_id

    def receive(self) -> dict | None:
        """Receives a message from the subscribed partition."""
        response = requests.get(self.url, params={"consumer_id": self.consumer_id})
        if response.status_code == 204:
            return None
        response.raise_for_status()
        return response.json()

    def acknowledge(self, message_id: int):
        """Acknowledges a message on the subscribed partition."""
        ack_url = self.ack_url_template.format(message_id=message_id)
        response = requests.post(ack_url, params={"consumer_id": self.consumer_id})
        response.raise_for_status()