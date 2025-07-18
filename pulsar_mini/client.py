
import requests

class PulsarClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def create_producer(self, topic: str) -> 'Producer':
        return Producer(self.base_url, topic)

    def subscribe(self, topic: str, consumer_id: str) -> 'Consumer':
        return Consumer(self.base_url, topic, consumer_id)

class Producer:
    def __init__(self, base_url: str, topic: str):
        self.url = f"{base_url}/topics/{topic}/messages"

    def send(self, data: str):
        response = requests.post(self.url, json={"data": data})
        response.raise_for_status()
        print(response.json())
        return response.json()["message_id"]

class Consumer:
    def __init__(self, base_url: str, topic: str, consumer_id: str):
        self.url = f"{base_url}/topics/{topic}/messages"
        self.ack_url_template = f"{base_url}/topics/{topic}/messages/{{message_id}}/ack"
        self.consumer_id = consumer_id

    def receive(self) -> dict:
        response = requests.get(self.url, params={"consumer_id": self.consumer_id})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def acknowledge(self, message_id: int):
        ack_url = self.ack_url_template.format(message_id=message_id)
        response = requests.post(ack_url, params={"consumer_id": self.consumer_id})
        response.raise_for_status()
