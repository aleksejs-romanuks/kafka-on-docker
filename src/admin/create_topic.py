from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})

topic = NewTopic("test_topic", num_partitions=3, replication_factor=1)
# topic1 = NewTopic("test_topic2", num_partitions=3, replication_factor=3)  # will fail!

new_topics = [topic]

# Call create_topics to asynchronously create topics. A dict
# of <topic,future> is returned.
result = admin.create_topics(new_topics)

# Wait for each operation to finish.
for topic, f in result.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))
