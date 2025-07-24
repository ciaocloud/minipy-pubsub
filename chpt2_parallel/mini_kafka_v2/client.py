import requests
from mini_kafka_v2.common import Record


class Producer:
    def __init__(self, broker_url: str, producer_id: str):
        self.broker_url = broker_url
        self.producer_id = producer_id

    def send(self, topic: str, payload: str, key: str = ""):
        message = Record(key=key, value=payload, topic=topic)
        url = f"{self.broker_url}/topics/{topic}/messages"
        response = requests.post(url, json=message.model_dump())
        # response = requests.post(url, params={"message": message, "key": key})
        return response.json()

class Consumer:
    def __init__(self, broker_url: str, consumer_id: str, topic: str, group_id: str):
        self.broker_url = broker_url
        self.url = f"{broker_url}/topics/{topic}/messages"
        self.ack_url_template = f"{broker_url}/topics/{topic}/messages/{{message_offset}}/ack"
        self.consumer_id = consumer_id
        self.group_id = group_id
        self.subscribe(topic, group_id)

    def subscribe(self, topic, group_id):
        subscribe_url = f"{self.broker_url}/topics/{topic}/groups/{group_id}/subscribe"
        response = requests.post(subscribe_url, params={"consumer_id": self.consumer_id})
        response.raise_for_status()
        return response.json()["assigned_partitions"]

    def poll(self):
        # return self.broker.consume_message(self.topic_name, self.consumer_id)
        response = requests.get(self.url, params={"consumer_id": self.consumer_id, "group_id": self.group_id})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def acknowledge(self, message_offset, partition_id):
        # self.broker.ack(self.topic_name, self.consumer_id, message_id)
        ack_url = self.ack_url_template.format(message_offset=message_offset)
        response = requests.post(ack_url, params={"consumer_id": self.consumer_id, "partition_id": partition_id})
        response.raise_for_status()
