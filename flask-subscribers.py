from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
from kafka import KafkaConsumer
import threading
import json
import random

app = Flask(__name__)
socketio = SocketIO(app)

# Static clusters for subscription
subscribers = {
    'sub1': set(),
    'sub2': set(),
    'sub3': set(),
    'sub4': set(),
    'sub5': set(),
    'sub6': set(),
    'sub7': set(),
    'sub8': set(),
    'sub9': set()
}

# Function to listen for Kafka messages and send them to subscribers
def kafka_consumer():
    consumer = KafkaConsumer(
        'sub1', 'sub2', 'sub3',  # Cluster 1 topics
        'sub4', 'sub5', 'sub6',  # Cluster 2 topics
        'sub7', 'sub8', 'sub9',  # Cluster 3 topics
        bootstrap_servers='localhost:9092',
        group_id='emoji-consumers',
        auto_offset_reset='earliest'
    )
    
    for message in consumer:
        try:
            emoji_data = json.loads(message.value.decode('utf-8'))
            print(f"Received message from topic {message.topic}: {emoji_data}")
            # Emit to the WebSocket room matching the topic
            socketio.emit('emoji_update', emoji_data, room=message.topic)
        except json.JSONDecodeError:
            print("Failed to decode message")

# WebSocket connection handlers
@socketio.on('connect')
def handle_connect():
    print("New client connected!")

@socketio.on('disconnect')
def handle_disconnect():
    print(f"Client disconnected: {request.sid}")
    # Remove client from all rooms it was in
    for cluster in subscribers:
        subscribers[cluster].discard(request.sid)

# WebSocket subscribe handler
@socketio.on('subscribe')
def handle_subscribe(data):
    cluster = data['cluster']
    if cluster in subscribers:
        subscribers[cluster].add(request.sid)
        join_room(cluster)  # Add client to the WebSocket room
        emit('message', f"Subscribed to {cluster}", room=request.sid)
    else:
        emit('message', 'Cluster does not exist.', room=request.sid)

# Register a new client to a random cluster
@app.route('/register', methods=['POST'])
def register_client():
    client_name = request.json.get('client_name')
    cluster = random.choice(list(subscribers.keys()))  # Assign client to a random cluster
    subscribers[cluster].add(client_name)
    return jsonify({"message": f"Client '{client_name}' registered to {cluster}.", "cluster": cluster})

# Deregister a client from a cluster
@app.route('/deregister', methods=['POST'])
def deregister_client():
    client_name = request.json.get('client_name')
    for cluster, clients in subscribers.items():
        if client_name in clients:
            clients.remove(client_name)
            return jsonify({"message": f"Client '{client_name}' deregistered from {cluster}."})
    
    return jsonify({"error": "Client not found."}), 404

if __name__ == "__main__":
    # Start Kafka consumer in a separate thread
    kafka_thread = threading.Thread(target=kafka_consumer)
    kafka_thread.daemon = True
    kafka_thread.start()
    
    # Run Flask app with WebSocket support
    socketio.run(app, port=5000)

