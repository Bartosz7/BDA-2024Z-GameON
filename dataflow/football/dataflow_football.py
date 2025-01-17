def run():
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
    from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
    from apache_beam.coders import PickleCoder
    from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

    import json
    import logging
    import uuid
    import ast

    PROJECT_ID = 'bda-gameon-demo'
    DATASET_ID = 'football'
    TOPIC_ID = f"projects/{PROJECT_ID}/topics/football-events-topic"
    OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET_ID}.processed_data_football"
    MATCHES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.matches_info"
    PREDICTIONS_TABLE = f"{PROJECT_ID}:{DATASET_ID}.predictions_football"
    MODEL_NAME = f"{PROJECT_ID}.{DATASET_ID}.random_forest_classifier"

    logging.basicConfig(level=logging.INFO)

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = "DataflowRunner"

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = 'europe-central2'
    gcp_options.job_name = 'football-dataflow-enrichment-pipeline-55'
    gcp_options.staging_location = 'gs://gameon-bucket-eu/staging'
    gcp_options.temp_location = 'gs://gameon-bucket-eu/temp'

    schema = {
        "fields": [
            {"name": "event_id", "type": "STRING"},
            {"name": "matchId", "type": "STRING"},
            {"name": "eventSec", "type": "FLOAT"},
            {"name": "home_shots", "type": "INTEGER"},
            {"name": "away_shots", "type": "INTEGER"},
            {"name": "home_accurate_shots", "type": "INTEGER"},
            {"name": "away_accurate_shots", "type": "INTEGER"},
            {"name": "home_passes", "type": "INTEGER"},
            {"name": "away_passes", "type": "INTEGER"},
            {"name": "home_goals", "type": "INTEGER"},
            {"name": "away_goals", "type": "INTEGER"},
            {"name": "home_fouls", "type": "INTEGER"},
            {"name": "away_fouls", "type": "INTEGER"},
            {"name": "home_duels_won", "type": "INTEGER"},
            {"name": "away_duels_won", "type": "INTEGER"},
            {"name": "home_possession_time", "type": "FLOAT"},
            {"name": "away_possession_time", "type": "FLOAT"},
            {"name": "goals_diff", "type": "INTEGER"},
            {"name": "passes_diff", "type": "INTEGER"},
            {"name": "fouls_diff", "type": "INTEGER"},
            {"name": "shots_diff", "type": "INTEGER"},
            {"name": "accurate_shots_diff", "type": "INTEGER"},
            {"name": "duels_won_diff", "type": "INTEGER"},
            {"name": "possession_time_diff", "type": "FLOAT"},
        ]
    }

    prediction_schema = {
    "fields": [
        {"name": "event_id", "type": "STRING"},
        {"name": "match_id", "type": "STRING"},
        {"name": "event_sec", "type": "FLOAT"},
        {"name": "predicted_result", "type": "STRING"},
        {"name": "prob_home_win", "type": "FLOAT"},
        {"name": "prob_away_win", "type": "FLOAT"},
        {"name": "prob_draw", "type": "FLOAT"},
    ]
}


    def parse_event_data(json_data):
        try:
            event = json.loads(json_data.decode("utf-8"))
            if not isinstance(event, dict):
                raise ValueError("Parsed data is not a dictionary")
            return event
        except Exception as e:
            logging.error(f"Failed to parse JSON: {e} | Data: {json_data}")
            return None
        

    def adjust_event_sec_for_2h(event):
        try:
            event["eventSec"] = event["eventSec"] + 3600 if event.get("matchPeriod") == "2H" else event["eventSec"]
            return event
        except Exception as e:
            logging.error(f"Failed to adjust eventSec: {e} | Event: {event}")
            return event


    def ensure_tags_list(event):
        tags_list = event.get("tagsList", "")
        try:
            if isinstance(tags_list, str) and tags_list.strip():
                event["tagsList"] = ast.literal_eval(tags_list)
            else:
                event["tagsList"] = []
        except Exception as e:
            logging.error(f"Failed to parse tagsList: {e} | Event: {event}")
            event["tagsList"] = []
        return event


    def generate_event_id(event):
        unique_id = str(uuid.uuid4())
        event["event_id"] = f"{event['matchId']}_{int(event['eventSec'])}_{unique_id}"
        return event


    class EnrichWithMatchInfoFn(beam.DoFn):
        def process(self, event, match_info):
            match_id = str(event.get('matchId')) if event.get('matchId') else None
            if not match_id or match_id not in match_info:
                event['side'] = "unknown"
                yield event
                return

            match = match_info[match_id]
            if event['teamId'] == match["team1_id"]:
                event['side'] = match["team1_side"]
            elif event['teamId'] == match["team2_id"]:
                event['side'] = match["team2_side"]
            else:
                event['side'] = "unknown"
            yield event


    class MatchStatsFn(beam.DoFn):
        match_stats_state = ReadModifyWriteStateSpec('match_stats', coder=PickleCoder())

        def process(self, element, match_stats_state=beam.DoFn.StateParam(match_stats_state)):
            try:
                match_id, event = element
                logging.info(f"Processing matchId: {match_id} with event: {event}")

                match_id = event.get('matchId')
                event_id = event.get('event_id')

                if not match_id:
                    logging.error("Missing matchId in event.")
                    return

                stats = match_stats_state.read() or {
                    "cumulative": {
                        "home_goals": 0, "away_goals": 0,
                        "home_passes": 0, "away_passes": 0,
                        "home_fouls": 0, "away_fouls": 0,
                        "home_shots": 0, "away_shots": 0,
                        "home_accurate_shots": 0, "away_accurate_shots": 0,
                        "home_duels_won": 0, "away_duels_won": 0,
                        "home_possession_time": 0, "away_possession_time": 0,
                    },
                    "diff": {
                        "goals_diff": 0,
                        "passes_diff": 0,
                        "fouls_diff": 0,
                        "shots_diff": 0,
                        "accurate_shots_diff": 0,
                        "duels_won_diff": 0,
                        "possession_time_diff": 0,
                    },
                    "last_event_time": None,
                    "last_event_side": None,
                }

                cumulative_stats = stats["cumulative"]
                diff_stats = stats["diff"]

                side = event.get("side", "unknown")
                event_name = event["eventName"].strip().lower()
                tags_list = event.get("tagsList", [])
                tags_list = list(map(str, tags_list))
                event_sec = event["eventSec"]

                if stats["last_event_time"] is not None:
                    time_diff = event_sec - stats["last_event_time"]
                    if stats["last_event_side"] == "home":
                        cumulative_stats["home_possession_time"] += max(0, time_diff)
                    elif stats["last_event_side"] == "away":
                        cumulative_stats["away_possession_time"] += max(0, time_diff)

                if stats["last_event_time"] is not None:
                    time_diff = event_sec - stats["last_event_time"]
                    if stats["last_event_side"] == "home":
                        cumulative_stats["home_possession_time"] += max(0, time_diff)
                    elif stats["last_event_side"] == "away":
                        cumulative_stats["away_possession_time"] += max(0, time_diff)

                if event_name == "shot":
                    if side == "home":
                        cumulative_stats["home_shots"] += 1
                        if "1801" in tags_list:
                            cumulative_stats["home_accurate_shots"] += 1
                            if "101" in tags_list:
                                cumulative_stats["home_goals"] += 1
                             
                    elif side == "away":
                        cumulative_stats["away_shots"] += 1
                        if "1801" in tags_list:
                            cumulative_stats["away_accurate_shots"] += 1
                            if "101" in tags_list:
                                cumulative_stats["away_goals"] += 1
                elif event_name == "pass":
                    if side == "home":
                        cumulative_stats["home_passes"] += 1
                    elif side == "away":
                        cumulative_stats["away_passes"] += 1
                elif event_name == "foul":
                    if side == "home":
                        cumulative_stats["home_fouls"] += 1
                    elif side == "away":
                        cumulative_stats["away_fouls"] += 1
                elif event_name == "duel" and "703" in tags_list:
                    if side == "home":
                        cumulative_stats["home_duels_won"] += 1
                    elif side == "away":
                        cumulative_stats["away_duels_won"] += 1

                diff_stats["goals_diff"] = cumulative_stats["home_goals"] - cumulative_stats["away_goals"]
                diff_stats["passes_diff"] = cumulative_stats["home_passes"] - cumulative_stats["away_passes"]
                diff_stats["fouls_diff"] = cumulative_stats["home_fouls"] - cumulative_stats["away_fouls"]
                diff_stats["shots_diff"] = cumulative_stats["home_shots"] - cumulative_stats["away_shots"]
                diff_stats["accurate_shots_diff"] = cumulative_stats["home_accurate_shots"] - cumulative_stats["away_accurate_shots"]
                diff_stats["duels_won_diff"] = cumulative_stats["home_duels_won"] - cumulative_stats["away_duels_won"]
                diff_stats["possession_time_diff"] = cumulative_stats["home_possession_time"] - cumulative_stats["away_possession_time"]
               
                stats["last_event_time"] = event_sec
                stats["last_event_side"] = side
                match_stats_state.write(stats)

                output = {
                    "event_id": event_id,
                    "matchId": match_id,
                    "eventSec": event["eventSec"],
                    **cumulative_stats,
                    **diff_stats,
                }

                logging.info(f"Generated output: {output}")
                yield output
            except Exception as e:
                logging.error(f"Failed to process event: {e} | Event: {event}")
                raise


    class PredictWithBigQueryProbaFn(beam.DoFn):
        def __init__(self, model_name):
            self.model_name = model_name

        def setup(self):
            from google.cloud import bigquery
            self.client = bigquery.Client()

        def process(self, element):
            """
            Query BigQuery ML model for predictions and extract probabilities.
            """
            try:
                event_json = json.dumps(element)

                query = f"""
                    WITH input_data AS (
                        SELECT
                            {element["home_goals"]} AS home_goals,
                            {element["away_goals"]} AS away_goals,
                            {element["home_passes"]} AS home_passes,
                            {element["away_passes"]} AS away_passes,
                            {element["home_fouls"]} AS home_fouls,
                            {element["away_fouls"]} AS away_fouls,
                            {element["home_shots"]} AS home_shots,
                            {element["away_shots"]} AS away_shots,
                            {element["home_accurate_shots"]} AS home_accurate_shots,
                            {element["away_accurate_shots"]} AS away_accurate_shots,
                            {element["home_duels_won"]} AS home_duels_won,
                            {element["away_duels_won"]} AS away_duels_won,
                            {element["home_possession_time"]} AS home_possession_time,
                            {element["away_possession_time"]} AS away_possession_time,
                            {element["goals_diff"]} AS goals_diff,
                            {element["passes_diff"]} AS passes_diff,
                            {element["fouls_diff"]} AS fouls_diff,
                            {element["shots_diff"]} AS shots_diff,
                            {element["accurate_shots_diff"]} AS accurate_shots_diff,
                            {element["duels_won_diff"]} AS duels_won_diff,
                            {element["possession_time_diff"]} AS possession_time_diff
                    )
                    SELECT
                        predicted_result,
                        ARRAY(
                            SELECT AS STRUCT
                                label,
                                prob
                            FROM UNNEST(predicted_result_probs)
                        ) AS probabilities
                    FROM ML.PREDICT(MODEL `{self.model_name}`, TABLE input_data)
                    """

                query_job = self.client.query(query)

                for row in query_job.result():
                    probabilities = {p["label"]: p["prob"] for p in row["probabilities"]}

                    yield {
                        "event_id": element["event_id"],
                        "match_id": element["matchId"],
                        "event_sec": element["eventSec"],
                        "predicted_result": row["predicted_result"],
                        "prob_home_win": probabilities.get("home", 0.0),
                        "prob_away_win": probabilities.get("away", 0.0),
                        "prob_draw": probabilities.get("draw", 0.0),
                    }
            except Exception as e:
                logging.error(f"Prediction error: {e} | Event: {element}")
                raise


    with beam.Pipeline(options=options) as pipeline:
        matches = (
            pipeline
            | "Read Matches from BigQuery" >> ReadFromBigQuery(
                query=f"""
                SELECT CAST(wyId AS STRING) AS matchId, team1_id, team2_id, team1_side, team2_side
                FROM `{MATCHES_TABLE}`
                """,
                use_standard_sql=True
            )
            | "Key Matches by matchId" >> beam.Map(lambda row: (row["matchId"], row))
        )

        matches_side_input = beam.pvalue.AsDict(matches)

        match_stats = (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=TOPIC_ID)
            | "Parse JSON" >> beam.Map(parse_event_data)
            | "Filter Non-Null Events" >> beam.Filter(lambda event: event is not None)
            | "Adjust EventSec for 2H" >> beam.Map(adjust_event_sec_for_2h)
            | "Ensure tagsList Is List" >> beam.Map(ensure_tags_list)
            | "Enrich with Match Info" >> beam.ParDo(EnrichWithMatchInfoFn(), matches_side_input)
            | "Generate Event ID" >> beam.Map(generate_event_id)
            | "Key by matchId" >> beam.Map(lambda event: (event['matchId'], event))
            | "Calculate Match Stats" >> beam.ParDo(MatchStatsFn())
        )

        match_stats | "Write Match Stats to BigQuery" >> WriteToBigQuery(
            table=OUTPUT_TABLE,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            custom_gcs_temp_location=gcp_options.temp_location,
        )

        predictions = (
            match_stats
            | "Predict with BigQuery ML" >> beam.ParDo(PredictWithBigQueryProbaFn(model_name=MODEL_NAME))
        )

        predictions | "Write Predictions to BigQuery" >> WriteToBigQuery(
            table=PREDICTIONS_TABLE,
            schema=prediction_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            custom_gcs_temp_location=gcp_options.temp_location,
        )


if __name__ == "__main__":
    run()