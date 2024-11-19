import os
import requests
import json
from google.cloud import pubsub_v1
from flask import Flask, request

# Flask app for Cloud Run
app = Flask(__name__)

# Configure your GCP Project and Pub/Sub topic
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'bda-gameon-demo')
TOPIC_ID = os.getenv('TOPIC_ID', 'football-events-topic')

# Initialize Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


@app.route("/", methods=["POST"])
def fetch_and_publish():
    try:
        # Retrieve parameters from the request payload
        data = request.json
        base_api_url = "https://big-data-project-api-248863766350.europe-west3.run.app/events"
        match_id = data.get("match_id", None)
        start_time = data.get("start_time", None)
        end_time = data.get("end_time", None)
        
        # Validate input
        if not match_id or not start_time or not end_time:
            return {
                "status": "error",
                "message": "Missing required parameters: match_id, start_time, end_time."
            }, 400
        
        # Construct the API URL
        api_url = f"{base_api_url}/{match_id}/{start_time}/{end_time}"
        
        # Fetch data from the constructed API URL
        response = requests.get(api_url)
        if response.status_code == 200:
            events = response.json().get("matches", {}).get(str(match_id), {}).get("events", {})
            
            # Publish each event to Pub/Sub
            for event_id, event_data in events.items():
                message_json = json.dumps(event_data).encode("utf-8")
                future = publisher.publish(topic_path, data=message_json)
                print(f"Published message ID: {future.result()}")

            return {
                "status": "success",
                "message": f"Data published to Pub/Sub for match_id {match_id} and time range {start_time}-{end_time}."
            }, 200
        else:
            return {
                "status": "error",
                "message": f"Failed to fetch data. Status code: {response.status_code}"
            }, 500
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)