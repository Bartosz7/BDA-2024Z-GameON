import os
import requests
import json
from google.cloud import pubsub_v1
from flask import Flask, request

# Flask app for Cloud Run
app = Flask(__name__)

# Configure your GCP Project and Pub/Sub topic
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'bda-gameon-demo')
TOPIC_ID = os.getenv('TOPIC_ID', 'formula-1-topic')

# Initialize Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


@app.route("/", methods=["POST"])
def fetch_and_publish():
    try:
        # Retrieve parameters from the request payload
        data = request.json
        base_api_url = "https://big-data-project-api-248863766350.europe-west3.run.app/laps"
        lap_number = data.get("lap_number", None)
        raceId = data.get("race_id", None)
        
        api_url = f"{base_api_url}/{raceId}/{lap_number}"
        
        # Fetch data from the constructed API URL
        response = requests.get(api_url)
        if response.status_code == 200:
            posts = response.json()
            # Publish each post to Pub/Sub
            for post in (posts if isinstance(posts, list) else [posts]):
                message_json = json.dumps(post).encode("utf-8")
                future = publisher.publish(topic_path, data=message_json)
                print(f"Published message ID: {future.result()}")
            return {"status": "success", "message": f"Data published to Pub/Sub from {api_url}."}, 200
        else:
            return {"status": "error", "message": f"Failed to fetch data. Status code: {response.status_code}"}, 500
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)