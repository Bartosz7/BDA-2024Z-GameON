"""
Defines a Dataflow pipeline to process cricket data from Pub/Sub messages (one over per message).
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions


PROJECT_ID = 'bda-gameon-demo'
TOPIC_ID = f'projects/{PROJECT_ID}/topics/cricket-topic'
DATASET_ID = "cricket"
OUTPUT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.processed_data"
MODEL = f"{PROJECT_ID}.{DATASET_ID}.xgboost_model"


def process_over(data):
    """
    Process a single over and calculate cumulative metrics using shared state.

    :param data: JSON data for the current over.
    """

    def transform_season(season: str) -> float:
        """Transform the season string to a single number."""
        parts = season.split("/")
        if len(parts) == 1:
            return int(parts[0])
        if len(parts) == 2:
            return (int(parts[0]) + 2000 + int(parts[1])) / 2
        
    def add_prediction(row):
        """
        Query the BigQuery ML model to get predictions for the given rows.

        :param row: Dictionary with the input features.
        :return: List of dictionaries with predictions added.
        """
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)

        query = f"""
        SELECT predicted_winner
        FROM ML.PREDICT(MODEL `{MODEL}`, (
            SELECT 
            {row['inning']} AS inning,
            {row['over']} AS over_number,
            {row['cumulative_score']} AS cumulative_score,
            {row['cumulative_wickets']} AS cumulative_wickets,
            {row['toss_decision']} AS toss_decision,
            {row['toss_winner']} AS toss_winner,
            {row['season']} AS season,
            {row['current_team']} AS current_team,
            {row['first_inning_total_score']} AS first_inning_total_score,
            {row['first_inning_total_wickets']} AS first_inning_total_wickets,
            {row['first_inning_run_rate']} AS first_inning_run_rate,
            '{row['team_1']}' AS team_1,
            '{row['team_2']}' AS team_2,
            {row['run_rate']} AS run_rate,
            {row['required_run_rate']} AS required_run_rate
        ))
        """

        query_job = client.query(query)
        results = list(query_job.result())
        if results is None or len(results) == 0:
            row['predicted_winner'] = None

        if results[0]['predicted_winner'] == 0:
            row['predicted_winner'] = row['team_1']
        else:
            row['predicted_winner'] = row['team_2']

        return row
    
    import json
    from google.cloud import storage

    def save_to_gcs(bucket_name, file_name, data):
        """
        Save a dictionary to GCS as a JSON file.
        
        :param bucket_name: The name of the GCS bucket.
        :param file_name: The file name (object name) in the bucket.
        :param data: The data to store (dictionary).
        """
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        serialized_data = json.dumps(data)
        blob.upload_from_string(serialized_data)
        print(f"Saved data to GCS: gs://{bucket_name}/{file_name}")

    def load_from_gcs(bucket_name, file_name):
        """
        Load a JSON file from GCS and deserialize it into a dictionary.
        
        :param bucket_name: The name of the GCS bucket.
        :param file_name: The file name (object name) in the bucket.
        :return: The deserialized dictionary, or an empty dictionary if the file does not exist.
        """
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        if not blob.exists():
            print(f"File not found: gs://{bucket_name}/{file_name}")
            return {}
        
        serialized_data = blob.download_as_string()
        return json.loads(serialized_data)

    PROJECT_ID = 'bda-gameon-demo'
    DATASET_ID = "cricket"
    MODEL = f"{PROJECT_ID}.{DATASET_ID}.xgboost_model"
    BUCKET_NAME = "bda-gameon-cricket-bucket"
    MATCH_HISTORY_FILE = "match_history.json"
    PREVIOUS_MATCH_ID_FILE = "previous_match_id.json"
    MEAN_OVER_SCORE = {"1": 6, "2": 7}
    MEAN_OVER_WICKETS = {"1": 0.5, "2": 1}

    match_history = load_from_gcs(BUCKET_NAME, MATCH_HISTORY_FILE)
    previous_match_id = load_from_gcs(BUCKET_NAME, PREVIOUS_MATCH_ID_FILE).get("match_id")

    try:
        parsed_data = json.loads(data.decode("utf-8"))
    except json.JSONDecodeError:
        return []
    
    match_id = parsed_data["match_id"]
    current_team = parsed_data["team"]
    teams = list(parsed_data["players"].keys())
    first_team = parsed_data["toss_winner"] if parsed_data["toss_decision"] == "field" else teams[1 - teams.index(parsed_data["toss_winner"])]
    second_team = teams[1 - teams.index(first_team)]
    inning = 1 + int(first_team == current_team)
    over_number = parsed_data["over_number"]
    deliveries = parsed_data["deliveries"]

    if match_id != previous_match_id:
        match_history = {
            f"{match_id}" : {
                "1" : {"score": 0, "wickets": 0},
                "2" : {"score": 0, "wickets": 0},
                "last_row": None
            }
        }
    else:
        last_inning = match_history[f"{match_id}"]["last_row"]["inning"]
        last_over_number = match_history[f"{match_id}"]["last_row"]["over"]

        if inning == last_inning and over_number > last_over_number + 1:
            match_history[f"{match_id}"][f"{inning}"]["score"] += int((over_number - last_over_number - 1) * MEAN_OVER_SCORE[f"{inning}"])
            match_history[f"{match_id}"][f"{inning}"]["wickets"] += int((over_number - last_over_number - 1) * MEAN_OVER_WICKETS[f"{inning}"])
        
        if inning < last_inning or (inning == last_inning and over_number <= last_over_number):
            return []
        
    cumulative_score = match_history[f"{match_id}"][f"{inning}"]["score"]
    cumulative_wickets = match_history[f"{match_id}"][f"{inning}"]["wickets"]

    processed_rows = []
    for delivery in deliveries:
        batter = delivery["batter"]
        bowler = delivery["bowler"]
        non_striker = delivery["non_striker"]
        runs_batter = delivery["runs"]["batter"]
        runs_extras = delivery["runs"]["extras"]
        runs_total = delivery["runs"]["total"]

        cumulative_score += runs_total
        if "wickets" in delivery:
            cumulative_wickets += len(delivery["wickets"])

        row = {
            "match_id": match_id,
            "inning": inning,
            "over": over_number,
            "batter": batter,
            "bowler": bowler,
            "non_striker": non_striker,
            "runs_batter": runs_batter,
            "runs_extras": runs_extras,
            "runs_total": runs_total,
            "cumulative_score": cumulative_score,
            "cumulative_wickets": min(cumulative_wickets, 10),
            "toss_decision": 1 if parsed_data["toss_decision"] == "field" else 0,
            "toss_winner": 1 if parsed_data["toss_winner"] != first_team else 0,
            "season": transform_season(parsed_data["season"]),
            "current_team": 1 if current_team != first_team else 0,
            "first_inning_total_score": match_history[f"{match_id}"]["1"]["score"] if inning == 2 else 0,
            "first_inning_total_wickets": match_history[f"{match_id}"]["1"]["wickets"] if inning == 2 else 0,
            "first_inning_run_rate": match_history[f"{match_id}"]["1"]["score"] / match_history[f"{match_id}"]["1"]["wickets"] if inning == 2 and match_history[f"{match_id}"]["1"]["wickets"] > 0 else 0,
            "team_1": first_team,
            "team_2": second_team,
            "run_rate": cumulative_score / (over_number + 1),
            "required_run_rate": (match_history[f"{match_id}"]["1"]["score"] - cumulative_score) / max(20 - over_number - 1, 1) if inning == 2 else 0,
        }
        processed_rows.append(row)

    row_with_prediction = add_prediction(processed_rows[-1])

    match_history[f"{match_id}"][f"{inning}"]["score"] = cumulative_score
    match_history[f"{match_id}"][f"{inning}"]["wickets"] = cumulative_wickets
    match_history[f"{match_id}"]["last_row"] = row_with_prediction

    save_to_gcs(BUCKET_NAME, MATCH_HISTORY_FILE, match_history)
    save_to_gcs(BUCKET_NAME, PREVIOUS_MATCH_ID_FILE, {"match_id": match_id})

    return [row_with_prediction]


def run():
    """Main function to execute the Dataflow pipeline."""
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = "DataflowRunner"

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = 'europe-west3'
    gcp_options.job_name = 'cricket-dataflow-pipeline'
    gcp_options.staging_location = 'gs://gameon-bucket-1/staging'
    gcp_options.temp_location = 'gs://gameon-bucket-1/temp'

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=TOPIC_ID)
            | "Process Over Data" >> beam.FlatMap(process_over)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema=(
                    "match_id:INTEGER, inning:INTEGER, over:INTEGER, batter:STRING, bowler:STRING, non_striker:STRING, "
                    "runs_batter:INTEGER, runs_extras:INTEGER, runs_total:INTEGER, cumulative_score:INTEGER, "
                    "cumulative_wickets:INTEGER, toss_decision:INTEGER, toss_winner:INTEGER, "
                    "season:FLOAT, current_team:INTEGER, first_inning_total_score:INTEGER, first_inning_total_wickets:INTEGER, "
                    "first_inning_run_rate:FLOAT, team_1:STRING, team_2:STRING, run_rate:FLOAT, required_run_rate:FLOAT, "
                    "predicted_winner:STRING"
                ),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
