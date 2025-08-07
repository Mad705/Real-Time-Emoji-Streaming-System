#!/usr/bin/env python3

import socketio
import requests
import threading
import time
import random
import os
import json

# URLs for server registration and sending emoji
SERVER_URL = "http://localhost:5000"
EMOJI_URL = "http://localhost:5001/send_emoji"

# File to store persistent user_id
USER_ID_FILE = "user_id.json"

# Socket.IO client setup
sio = socketio.Client()

# Variable to hold the cluster assigned by the server
assigned_cluster = None

# Flag to control the emoji sending thread
send_data_flag = False

# List of sample emoji types
emoji_types = ["clap", "heart", "smile", "thumbs_up", "laugh"]


def get_persistent_user_id():
    user_id = random.randint(1, 1000)
    with open(USER_ID_FILE, 'w') as f:
        json.dump({"user_id": user_id}, f)
    print(f"Generated new user_id: {user_id}")  # Debug message
    return user_id


# Assign a persistent user_id
user_id = get_persistent_user_id()


def send_emoji_data():
    """
    Continuously sends emoji data to the server every second with a consistent user_id.
    """
    while send_data_flag:
        try:
            emoji_type = random.choice(emoji_types)
            timestamp = int(time.time())
            data = {
                "user_id": str(user_id),  # Use the consistent user_id
                "emoji_type": emoji_type,
                "timestamp": timestamp
            }
            response = requests.post(EMOJI_URL, json=data)
            print(f"Sent: {data}, Response: {response.status_code}")
        except Exception as e:
            print(f"Error sending emoji data: {e}")
        time.sleep(1)  # Adjust the delay to control sending rate


@sio.event
def connect():
    """
    Event handler for successful connection to the server.
    Subscribes to the assigned cluster and starts sending emoji data.
    """
    print("Connected to server")
    if assigned_cluster:
        print(f"Subscribing to cluster: {assigned_cluster}")
        sio.emit('subscribe', {'cluster': assigned_cluster})
        # Start sending emoji data
        global send_data_flag
        if not send_data_flag:
            send_data_flag = True
            threading.Thread(target=send_emoji_data, daemon=True).start()


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
    max_display = 50  # Limit the number of emojis displayed
    print(emoji_char * min(aggregated_count, max_display))


@sio.event
def disconnect():
    """
    Event handler for disconnection from the server.
    Stops the emoji sending thread.
    """
    global send_data_flag
    print("Disconnected from server")
    send_data_flag = False


def register_client(client_name):
    """
    Registers the client with the server via an HTTP POST request.
    """
    global assigned_cluster
    try:
        response = requests.post(f"{SERVER_URL}/register", json={'client_name': client_name})
        if response.status_code == 200:
            cluster_info = response.json()
            assigned_cluster = cluster_info['cluster']
            print(f"Server assigned cluster: {assigned_cluster}")
        else:
            print(f"Registration failed with status code: {response.status_code}")
    except Exception as e:
        print(f"Error during registration: {e}")


def main():
    """
    Main function to register the client and connect to the server.
    """
    client_name = f"client_{user_id}"  # Unique client name based on user_id

    # Step 1: Register the client
    register_client(client_name)

    if not assigned_cluster:
        print("Cannot proceed without a cluster assignment.")
        return

    # Step 2: Connect to the Flask server via Socket.IO
    try:
        sio.connect(SERVER_URL)
        # Keep the client listening for events
        sio.wait()
    except Exception as e:
        print(f"Error connecting to Socket.IO server: {e}")


if __name__ == "__main__":
    main()

