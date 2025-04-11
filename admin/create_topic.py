from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient({
    'bootstrap.servers': 'kafka:9093'
})

topic_name = "TRANSACTIONS_CONFIRMED"
new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)

fs = admin_client.create_topics([new_topic])

for topic, f in fs.items():
    try:
        f.result()  # Wait for the topic creation to complete
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")