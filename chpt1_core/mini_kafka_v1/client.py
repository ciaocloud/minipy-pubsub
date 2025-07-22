import requests

# class Client:
#     def __init__(self, broker_url: str):
#         self.broker_url = broker_url

    # def create_producer

class Producer:
    def __init__(self, broker_url: str, producer_id: str):
        self.broker_url = broker_url
        self.producer_id = producer_id

    def send(self, topic_name: str, message: str):
        url = f"{self.broker_url}/topics/{topic_name}/messages"
        response = requests.post(url, params={"message": message})
        return response.json()["message_id"]

class Consumer:
    def __init__(self, broker_url: str, consumer_id: str, topic: str):
        self.url = f"{broker_url}/topics/{topic}/messages"
        self.ack_url_template = f"{broker_url}/topics/{topic}/messages/{{message_id}}/ack"
        # self.topic_name = topic_name
        self.consumer_id = consumer_id

    def poll(self):
        # return self.broker.consume_message(self.topic_name, self.consumer_id)
        response = requests.get(self.url, params={"consumer_id": self.consumer_id})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def acknowledge(self, message_id):
        # self.broker.ack(self.topic_name, self.consumer_id, message_id)
        ack_url = self.ack_url_template.format(message_id=message_id)
        response = requests.post(ack_url, params={"consumer_id": self.consumer_id})
        response.raise_for_status()
