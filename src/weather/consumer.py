from confluent_kafka import Consumer
import json

topic = "weather"

consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'weather-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([topic])

categories = {
    1: "calm",
    2: "light",
    3: "moderate",
    4: "fresh",
    5: "strong",
    6: "gale",
    7: "storm",
    8: "hurricane",
}

def parsing(json_data):
    dataseries = json_data["dataseries"]
    for datasery in dataseries:
        date = datasery["date"]
        wind = datasery["wind10m_max"]
        print("Date {}, wind category {}.".format(date, categories.get(wind)))


while True:
    msg = consumer.poll()

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    msg_decoded = msg.value().decode('utf-8')
    msg_json_converted = json.loads(msg_decoded)
    parsing(msg_json_converted)

consumer.close()
