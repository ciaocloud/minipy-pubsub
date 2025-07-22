class Topic:
    def __init__(self, name: str):
        self.name = name
        self.messages = list()
        self.write_offset = 0
        self.cursors = dict() ## read offset for each consumer

    def produce(self, message: str):
        self.messages.append(message)
        self.write_offset += 1
        return self.write_offset - 1

    def consume(self, consumer_id: str):
        offset = 0
        if consumer_id in self.cursors:
            offset = self.cursors[consumer_id] + 1
        if offset >= self.write_offset:
            return None
        return offset, self.messages[offset]

    def commit(self, consumer_id, message_id):
        self.cursors[consumer_id] = message_id

class Broker:
    def __init__(self):
        self.topics = {}

    def create_topic(self, topic_name):
        if not topic_name in self.topics:
            self.topics[topic_name] = Topic(topic_name)

    def publish_message(self, topic_name, message):
        if not topic_name in self.topics:
            self.create_topic(topic_name)
        topic = self.topics[topic_name]
        return topic.produce(message)

    def consume_message(self, topic_name: str, consumer_id: str):
        topic = self.topics[topic_name]
        result = topic.consume(consumer_id)
        if not result:
            print("No new messages")
            return None
        #     raise Exception("No new messages")
        message_offset, message = result
        return {"message_id": message_offset, "data": message}

    def ack(self, topic_name, consumer_id, message_id):
        topic = self.topics[topic_name]
        topic.commit(consumer_id, message_id)
