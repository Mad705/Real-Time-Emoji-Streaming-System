from kafka import KafkaConsumer, KafkaProducer

def main_publisher_to_clusters():
    consumer = KafkaConsumer(
        'main-publisher',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='main-publisher-group'
    )
    
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    clusters = ['cluster1', 'cluster2', 'cluster3']
    
    for message in consumer:
        # Publish to all clusters
        for cluster in clusters:
            producer.send(cluster, message.value)

if __name__ == "__main__":
    main_publisher_to_clusters()

