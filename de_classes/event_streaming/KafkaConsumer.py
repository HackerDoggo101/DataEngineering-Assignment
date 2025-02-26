## Author: Yam Jason

from kafka import KafkaConsumer
import json

class KafkaConsumerClass:
    def __init__(self, bootstrap_servers, topic_name, group_id='my-group'):
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize data from JSON
        )

    def consume_messages(self):
        for message in self.consumer:
            print(f"Received message: {message.value}")

    def close(self):
        self.consumer.close()