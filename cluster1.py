from kafka import KafkaConsumer, KafkaProducer
import json

# Create a Kafka consumer to listen to the 'cluster1' topic
consumer = KafkaConsumer(
    'cluster1',  # Consume from 'cluster1'
    bootstrap_servers='localhost:9092',
    group_id='emoji-consumers',
    auto_offset_reset='earliest'
)

# Create a Kafka producer to send messages to 'sub1', 'sub2', and 'sub3' topics
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Process messages from 'cluster1'
for message in consumer:
    # Parse the JSON message from Kafka
    emoji_data = json.loads(message.value.decode('utf-8'))

    # Print the parsed data from 'cluster1'
    print(f"Received from cluster1: {emoji_data}")

    # Send the same message to 'sub1', 'sub2', and 'sub3'
    producer.send('sub1', value=emoji_data)
    producer.send('sub2', value=emoji_data)
    producer.send('sub3', value=emoji_data)

    print(f"Sent data to sub1, sub2, and sub3: {emoji_data}")

