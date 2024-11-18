import os
import requests
import json
import base64
from google.cloud import pubsub_v1

# Configure your GCP Project and Pub/Sub topic
PROJECT_ID = 'bda-gameon-demo'
TOPIC_ID = 'data-ingestion-topic-test'
API_URL = 'https://jsonplaceholder.typicode.com/posts'

# Initialize Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def fetch_and_publish():
    # Fetch data from JSONPlaceholder API
    response = requests.get(API_URL)
    if response.status_code == 200:
        posts = response.json()
        for post in posts:
            # Convert each post to JSON string and encode as bytes
            message_json = json.dumps(post).encode("utf-8")
            # Publish message to Pub/Sub
            future = publisher.publish(topic_path, data=message_json)
            print(f"Published message ID: {future.result()}")
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")


if __name__ == "__main__":
    fetch_and_publish()
