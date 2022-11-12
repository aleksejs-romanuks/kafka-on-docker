from confluent_kafka.admin import AdminClient

a = AdminClient({'bootstrap.servers': '127.0.0.1:9092'})

topic_to_delete = ["test_topic1", "test_topic2"]

# Call create_topics to asynchronously create topics. A dict
# of <topic,future> is returned.
result = a.delete_topics(topic_to_delete)

# Wait for each operation to finish.
for topic, f in result.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} deleted".format(topic))
    except Exception as e:
        print("Failed to delete topic {}: {}".format(topic, e))
