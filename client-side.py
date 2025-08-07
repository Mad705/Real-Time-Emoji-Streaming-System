import socketio
import requests

# Connect to the Flask server
sio = socketio.Client()

# Variable to hold the cluster assigned by the server
assigned_cluster = None

@sio.event
def connect():
    print("Connected to server")
    if assigned_cluster:
        # Subscribe to the assigned cluster after connection
        print(f"Subscribing to cluster: {assigned_cluster}")
        sio.emit('subscribe', {'cluster': assigned_cluster})

@sio.event
def new_registration(data):
    global assigned_cluster
    assigned_cluster = data['cluster']  # Save assigned cluster
    print(f"Registered to cluster: {assigned_cluster}")
    # Emit the subscribe event only if connected
    if sio.connected:
        sio.emit('subscribe', {'cluster': assigned_cluster})

@sio.event
def emoji_update(data):
    """
    Event handler for receiving aggregated emoji data from the server.
    """
    emoji_map = {
        "thumbs_up": "👍",
        "clap": "👏",
        "heart": "❤️",
        "smile": "😊",
        "laugh": "😂"
    }
    emoji_type = data.get("emoji_type", "unknown")
    aggregated_count = data.get("aggregated_count", 0)
    
    # Get the corresponding emoji or a default string
    emoji_char = emoji_map.get(emoji_type, "❓")
    
    # Print the emoji the number of times specified by aggregated_count
    print(emoji_char * aggregated_count)

@sio.event
def disconnect():
    print("Disconnected from server")

# Register the client with the server
def register_client(client_name):
    global assigned_cluster
    response = requests.post('http://localhost:5000/register', json={'client_name': client_name})
    if response.status_code == 200:
        cluster_info = response.json()
        assigned_cluster = cluster_info['cluster']  # Save assigned cluster
        print(f"Server assigned cluster: {assigned_cluster}")
    else:
        print("Registration failed")

# Register the client
client_name = 'client1'
register_client(client_name)

# Connect to the Flask server
sio.connect('http://localhost:5000')

# Keep the client listening for events
sio.wait()
