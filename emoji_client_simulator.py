#!/usr/bin/env python3

import requests
import threading
import time
import random

URL = "http://localhost:5001/send_emoji"

# List of sample emoji types
emoji_types = ["clap", "heart", "smile", "thumbs_up", "laugh"]

# Function to send emoji data
def send_emoji_data():
    user_id = random.randint(1, 1000)
    emoji_type = random.choice(emoji_types)
    timestamp = int(time.time())
    data = {
        "user_id": str(user_id),
        "emoji_type": emoji_type,
        #"emoji_type": "laugh",
        "timestamp": timestamp
    }
    response = requests.post(URL, json=data)
    print(data)
    # print(f"Sent: {data}, Response: {response.json()}")

# Simulate multiple clients
def simulate_clients():
    while True:
        threads = []
        for _ in range(200):  # Spawn 200 threads to simulate 200 clients
            thread = threading.Thread(target=send_emoji_data)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

if __name__ == "__main__":
    simulate_clients()

