from mini_kafka_v0.server import Broker

class Producer:
    def __init__(self, broker: Broker, producer_id: str):
        self.broker = broker
        self.producer_id = producer_id

    def send(self, topic: str, message: str):
        return self.broker.publish_message(topic, message)

class Consumer:
    def __init__(self, broker: Broker, consumer_id: str, topic_name: str):
        self.broker = broker
        self.topic_name = topic_name
        self.consumer_id = consumer_id

    def poll(self):
        return self.broker.consume_message(self.topic_name, self.consumer_id)
        # self.ack(message_offset)

    def acknowledge(self, message_id):
        self.broker.ack(self.topic_name, self.consumer_id, message_id)