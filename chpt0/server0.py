from typing import Dict, List, Optional, Callable
import client0
from fastapi import FastAPI, HTTPException

class Broker:
    def __init__(self):
        self.topics = Dict[str, Topic]

    def create_topic(self, topic_name):
        if topic_name in self.topics:
            return {"status": "error", "message": f"Topic {topic_name} already exits"}
        self.topics[topic_name] = Topic(topic_name)
        return {"status": "success", "message": f"Created topic {topic_name}"}


# class Topic:
#     def __init__(self, name: str):
#         self.name = name
#         self.messages = list()
#         self.write_offset = 0
#         self.cursors = dict() ## read offset for each consumer
#
#     def produce(self, payload):
#         self.messages.append(payload)
#         self.write_offset += 1
#         return self.write_offset - 1
#
#     def consume(self, consumer_id: str):
#         offset = 0
#         if consumer_id in self.cursors:
#             offset = self.cursors[consumer_id]
#         if offset >= self.write_offset:
#             return None
#         return offset, self.messages[offset]
#
#     def commit(self, consumer_id, message_id):
#         self.cursors[consumer_id] = message_id

# app = FastAPI()
broker = Broker()

# @app.post("/topics/{topic}/messages")
# def send_handler(topic: str, message: str):
#     _topic = broker.topics[topic]
#     message_id = _topic.produce(message)
#     return {"message_id": message_id}
#
# @app.get("/topics/{topic}/message")
# def poll_handler(topic: str, consumer_id: str):
#     _topic = broker.topics[topic]
#     result = _topic.consume(consumer_id)
#     if not result:
#         raise HTTPException(status_code=404, detail="No new messages")
#     message_id, payload = result
#     return {"message_id": message_id, "payload": payload}
#
# @app.post("/topics/{topic}/messages/{message_id}/ack")
# def ack_handler(topic: str, message_id: int, consumer_id: str):
#     _topic = broker.topics[topic]
#     _topic.commit(consumer_id, message_id)
#     return {"status": "ok"}
