from kafka import KafkaProducer

producer = KafkaProducer()
print(producer.send('quickstart', b'some_message_bytes').get(timeout=30))