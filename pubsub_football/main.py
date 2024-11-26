import os
import requests
import json
from google.cloud import pubsub_v1
from flask import Flask, request

app = Flask(__name__)

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'bda-gameon-demo')
TOPIC_ID = os.getenv('TOPIC_ID', 'football-events-topic')

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


@app.route("/", methods=["POST"])
def fetch_and_publish():
    try:
        data = request.json
        base_api_url = "https://big-data-project-api-248863766350.europe-west3.run.app/events"
        match_id = data.get("match_id", None)
        start_time = data.get("start_time", None)
        end_time = data.get("end_time", None)
        
        if not match_id or not start_time or not end_time:
            return {
                "status": "error",
                "message": "Missing required parameters: match_id, start_time, end_time."
            }, 400
        
        api_url = f"{base_api_url}/{match_id}?start_time={start_time}&end_time={end_time}"
        
        response = requests.get(api_url)
        if response.status_code == 200:
            response_json = response.json()
            if "error" in response_json:
                return {"status": "error", "message": response_json["error"]}, 500
            
            events = response_json.get("matches", {}).get(str(match_id), {}).get("events", {})
            
            for event_id, event_data in events.items():
                event_data["matchId"] = match_id
                
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
                "message": f"Failed to fetch data. Status code: {response.status_code}.",
            }, 500
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)