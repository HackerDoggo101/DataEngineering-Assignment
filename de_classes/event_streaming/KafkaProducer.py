## Author: Yam Jason

from kafka import KafkaProducer
import json

class KafkaProducerClass:
    def __init__(self, bootstrap_servers, topic_name):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic_name = topic_name

    def produce_message(self, message):
        self.producer.send(self.topic_name, value=message)
        self.producer.flush()

    def close(self):
        self.producer.close()

