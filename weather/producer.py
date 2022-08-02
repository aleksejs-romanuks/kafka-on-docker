from confluent_kafka import Producer
import requests

producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})

response_format = "json"
lon = "24.10"
lat = "59.94"
product = "civillight"

response = requests.get(
    "http://www.7timer.info/bin/api.pl?lon={}&lat={}product={}&output={}".format(
        lon, lat, product, response_format
    )
)

json_msg = response.json()


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


producer.produce('simple_topic', json_msg.encode('utf-8'), callback=delivery_report)


producer.flush()
