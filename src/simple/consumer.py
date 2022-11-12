from confluent_kafka import Consumer


consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'simple',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['test_topic'])

while True:
    msg = consumer.poll()

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

consumer.close()
