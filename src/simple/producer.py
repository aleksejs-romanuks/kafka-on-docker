from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})

msg1 = "Hello!"
msg2 = "I'm working!"


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {}, parition [{}]'.format(msg.topic(), msg.partition()))


for data in [msg1, msg2]:

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    producer.produce('test_topic', data.encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
producer.flush()
