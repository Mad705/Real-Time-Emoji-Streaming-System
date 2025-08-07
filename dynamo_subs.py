from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, join_room
from kafka import KafkaConsumer
import threading
import json

# Static configuration
subscriber_capacity = 2  # Maximum clients per subscriber
subscribers = {  # Subscriber data with assigned port, client set, and client count
    'sub1': {'port': 6001, 'clients': set(), 'client_count': 0},
    'sub2': {'port': 6002, 'clients': set(), 'client_count': 0},
    'sub3': {'port': 6003, 'clients': set(), 'client_count': 0},
    'sub4': {'port': 6004, 'clients': set(), 'client_count': 0},
    'sub5': {'port': 6005, 'clients': set(), 'client_count': 0},
    'sub6': {'port': 6006, 'clients': set(), 'client_count': 0},
    'sub7': {'port': 6007, 'clients': set(), 'client_count': 0},
    'sub8': {'port': 6008, 'clients': set(), 'client_count': 0},
    'sub9': {'port': 6009, 'clients': set(), 'client_count': 0}
}

# Flask app and WebSocket initialization
app = Flask(__name__)
socketio = SocketIO(app)

# Function to listen for Kafka messages and send them to subscribers
def kafka_consumer():
    consumer = KafkaConsumer(
        *subscribers.keys(),  # Subscribe to all topics
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

# WebSocket handlers
@socketio.on('connect')
def handle_connect():
    print("New client connected!")
    # Print current client counts for all subscribers
    print("Current subscriber client counts:")
    for cluster, data in subscribers.items():
        print(f"{cluster}: {data['client_count']}/{subscriber_capacity}")

@socketio.on('disconnect')
def handle_disconnect():
    print(f"Client disconnected: {request.sid}")
    # Find the cluster where the client was registered
    for cluster in subscribers:
        if request.sid in subscribers[cluster]['clients']:
            # Remove the client and decrement the client count
            subscribers[cluster]['clients'].remove(request.sid)
            subscribers[cluster]['client_count'] -= 1  # Decrement client count
            print(f"Client removed from {cluster} (Client count: {subscribers[cluster]['client_count']})")
            break  # Exit loop once the client is removed
    
    # Print the current client counts for all subscribers after a disconnect
    print("Updated subscriber client counts after disconnection:")
    for cluster, data in subscribers.items():
        print(f"{cluster}: {data['client_count']}/{subscriber_capacity}")

@socketio.on('subscribe')
def handle_subscribe(data):
    cluster = data['cluster']
    if cluster in subscribers:
        join_room(cluster)  # Join WebSocket room
        emit('message', f"Subscribed to {cluster}", room=request.sid)
    else:
        emit('message', 'Cluster does not exist.', room=request.sid)

# Assign clients to clusters based on capacity
@app.route('/register', methods=['POST'])
def register_client():
    client_name = request.json.get('client_name')

    # Print current subscriber client counts
    print("Current subscriber client counts:")
    for cluster, data in subscribers.items():
        print(f"{cluster}: {data['client_count']}/{subscriber_capacity}")
    
    # Check all clusters for available capacity
    for cluster, data in subscribers.items():
        if data['client_count'] < subscriber_capacity:
            # Assign client to this cluster
            data['clients'].add(client_name)
            data['client_count'] += 1  # Increment the client count for that cluster
            print(f"Client '{client_name}' registered to {cluster}. (Client count: {data['client_count']})")
            return jsonify({"message": f"Client '{client_name}' registered to {cluster}.", 
                            "cluster": cluster, 
                            "port": data['port']})
    
    # If no free spots, return error
    return jsonify({"error": "No clusters available with capacity."}), 503

@app.route('/deregister', methods=['POST'])
def deregister_client():
    client_name = request.json.get('client_name')
    print(f"Received deregistration request for {client_name}")
    
    # Find the cluster where the client is registered and remove them
    for cluster, data in subscribers.items():
        if client_name in data['clients']:
            data['clients'].remove(client_name)
            data['client_count'] -= 1  # Decrement the client count for that cluster
            print(f"Client '{client_name}' deregistered from {cluster}. (Client count: {data['client_count']})")
            return jsonify({"message": f"Client '{client_name}' deregistered from {cluster}."})

    return jsonify({"error": "Client not found."}), 404

def start_subscriber(cluster, port):
    subscriber_app = Flask(cluster)
    subscriber_socketio = SocketIO(subscriber_app)

    @subscriber_socketio.on('connect')
    def handle_connect():
        print(f"Client connected to {cluster} on port {port}.")

    @subscriber_socketio.on('disconnect')
    def handle_disconnect():
        print(f"Client disconnected from {cluster} on port {port}.")

    subscriber_socketio.run(subscriber_app, port=port)

if __name__ == "__main__":
    # Start Kafka consumer in a separate thread
    kafka_thread = threading.Thread(target=kafka_consumer)
    kafka_thread.daemon = True
    kafka_thread.start()

    # Start a separate Flask app for each subscriber
    for cluster, data in subscribers.items():
        threading.Thread(
            target=start_subscriber,
            args=(cluster, data['port']),
            daemon=True
        ).start()

    # Run the main Flask app
    socketio.run(app, port=5000)

