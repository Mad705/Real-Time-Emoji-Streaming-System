#!/usr/bin/env python3

from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import atexit

app = Flask(__name__)

# Initialize Kafka producer with buffering
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500  # Buffer messages for 500 ms
)

# Endpoint to receive emoji data
@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    data = request.get_json()
    # Example data: {"user_id": "123", "emoji_type": "clap", "timestamp": int(time.time())}
    if not all(k in data for k in ("user_id", "emoji_type", "timestamp")):
        return jsonify({"error": "Missing data"}), 400

    # Send data to Kafka
    producer.send('topic1', data)
    return jsonify({"status": "sent"}), 200

# Graceful shutdown of producer
@atexit.register
def close_producer():
    producer.flush()
    producer.close()

if __name__ == "__main__":
    app.run(port=5001)

