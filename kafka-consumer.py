from kafka import KafkaConsumer
consumer = KafkaConsumer('quickstart')
for message in consumer:
    print (message.topic)
    print (message.value)