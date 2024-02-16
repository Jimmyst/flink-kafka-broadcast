from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# produce json messages
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# produce asynchronously
for _ in range(10):
    producer.send('transactions', b'{"uid": "uid_4", "price": 1}')

# block until all async messages are sent
producer.flush()
