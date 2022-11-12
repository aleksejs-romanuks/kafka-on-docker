from confluent_kafka import Producer
import requests
import json

producer = Producer({'bootstrap.servers': '127.0.0.1:9092'})
topic = "weather"

# Option 1:
# response_format = "json"
# lon = "24.10"
# lat = "59.94"
# product = "civillight"

# url = "http://www.7timer.info/bin/api.pl?lon={}&lat={}&product={}&output={}".format(
#     lon, lat, product, response_format
# )

# response = requests.get(url)

# Option 2:
params = {
    "lon": "24.10",
    "lat": "59.94",
    "product": "civillight",
    "output": "json",
}

url = "http://www.7timer.info/bin/api.pl"
response = requests.get(url, params=params)

try:
    json_msg = response.json()
    print(json_msg)
except:
    raise Exception("Response not in JSON form. Response: '{}'.".format(response.text))

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {}, partition [{}]'.format(msg.topic(), msg.partition()))


producer.produce(topic, json.dumps(json_msg).encode('utf-8'), callback=delivery_report)


producer.flush()
