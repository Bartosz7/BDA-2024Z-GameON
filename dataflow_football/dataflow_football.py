def run():
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
    from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
    import json
    import logging
    import time

    PROJECT_ID = 'bda-gameon-demo'
    DATASET_ID = 'football'
    TOPIC_ID = f"projects/{PROJECT_ID}/topics/football-events-topic"
    OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET_ID}.processed_data"
    MATCHES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.matches_info"

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = 'europe-west3'
    gcp_options.job_name = 'football-dataflow-enrichment-pipeline-16'
    gcp_options.staging_location = 'gs://gameon-bucket-eu/staging'
    gcp_options.temp_location = 'gs://gameon-bucket-eu/temp'

    schema = {
        "fields": [
            {"name": "matchId", "type": "STRING"},
            {"name": "eventSec", "type": "INTEGER"},
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
            {"name": "home_shots_last_5min", "type": "INTEGER"},
            {"name": "away_shots_last_5min", "type": "INTEGER"},
            {"name": "home_passes_last_5min", "type": "INTEGER"},
            {"name": "away_passes_last_5min", "type": "INTEGER"},
            {"name": "home_goals_last_5min", "type": "INTEGER"},
            {"name": "away_goals_last_5min", "type": "INTEGER"},
            {"name": "home_fouls_last_5min", "type": "INTEGER"},
            {"name": "away_fouls_last_5min", "type": "INTEGER"},
        ]
    }

    def parse_event_data(json_data):
        try:
            return json.loads(json_data.decode("utf-8"))
        except Exception as e:
            logging.error(f"Failed to parse JSON: {e}")
            return None

    class EnrichWithMatchInfoFn(beam.DoFn):
        def process(self, event, match_info):
            match_id = event.get("matchId")
            if not match_id or match_id not in match_info:
                event["side"] = "unknown"
                yield event
                return

            match = match_info[match_id]
            if event["teamId"] == match["team1_id"]:
                event["side"] = match["team1_side"]
            elif event["teamId"] == match["team2_id"]:
                event["side"] = match["team2_side"]
            else:
                event["side"] = "unknown"
            yield event

    class MatchStatsFn(beam.DoFn):
        def __init__(self):
            self.match_stats = {}

        def process(self, element):
            match_id, events = element

            if match_id not in self.match_stats:
                self.match_stats[match_id] = {
                    "cumulative": {
                        "home_goals": 0, "away_goals": 0,
                        "home_passes": 0, "away_passes": 0,
                        "home_fouls": 0, "away_fouls": 0,
                        "home_shots": 0, "away_shots": 0,
                        "home_accurate_shots": 0, "away_accurate_shots": 0,
                    },
                    "rolling_window": [],
                }

            stats = self.match_stats[match_id]
            cumulative_stats = stats["cumulative"]
            rolling_window = stats["rolling_window"]

            current_time = time.time()

            for event in events:
                event_sec = event["eventSec"]
                side = event.get("side", "unknown")
                event_name = event["eventName"].strip().lower()

                if event_name == "shot":
                    if side == "home":
                        cumulative_stats["home_shots"] += 1
                        if 1801 in event.get("tagsList", []):
                            cumulative_stats["home_accurate_shots"] += 1
                    elif side == "away":
                        cumulative_stats["away_shots"] += 1
                        if 1801 in event.get("tagsList", []):
                            cumulative_stats["away_accurate_shots"] += 1
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
                elif "goal" in event.get("tagsList", []):
                    if side == "home" and 1801 in event.get("tagsList", []):
                        cumulative_stats["home_goals"] += 1
                    elif side == "away" and 1801 in event.get("tagsList", []):
                        cumulative_stats["away_goals"] += 1

                rolling_window.append((current_time, event))

            rolling_window = [e for t, e in rolling_window if current_time - t <= 300]
            stats["rolling_window"] = rolling_window

            rolling_stats = {
                "home_shots_last_5min": sum(1 for _, e in rolling_window if e["side"] == "home" and e["eventName"] == "shot"),
                "away_shots_last_5min": sum(1 for _, e in rolling_window if e["side"] == "away" and e["eventName"] == "shot"),
                "home_passes_last_5min": sum(1 for _, e in rolling_window if e["side"] == "home" and e["eventName"] == "pass"),
                "away_passes_last_5min": sum(1 for _, e in rolling_window if e["side"] == "away" and e["eventName"] == "pass"),
                "home_goals_last_5min": sum(1 for _, e in rolling_window if e["side"] == "home" and e["eventName"] == "goal"),
                "away_goals_last_5min": sum(1 for _, e in rolling_window if e["side"] == "away" and e["eventName"] == "goal"),
                "home_fouls_last_5min": sum(1 for _, e in rolling_window if e["side"] == "home" and e["eventName"] == "foul"),
                "away_fouls_last_5min": sum(1 for _, e in rolling_window if e["side"] == "away" and e["eventName"] == "foul"),
            }

            output = {
                "matchId": match_id,
                **cumulative_stats,
                **rolling_stats,
            }

            yield output

    with beam.Pipeline(options=options) as pipeline:
        matches = (
            pipeline
            | "Read Matches from BigQuery" >> ReadFromBigQuery(
                query=f"""
                SELECT wyId AS matchId, team1_id, team2_id, team1_side, team2_side
                FROM `{MATCHES_TABLE}`
                """,
                use_standard_sql=True
            )
            | "Key Matches by matchId" >> beam.Map(lambda row: (row["matchId"], row))
        )

        matches_side_input = beam.pvalue.AsDict(matches)

        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=TOPIC_ID)
            | "Parse JSON" >> beam.Map(parse_event_data)
            | "Filter Valid Events" >> beam.Filter(lambda x: x is not None)
            | "Assign Timestamps" >> beam.Map(lambda event: beam.window.TimestampedValue(event, event['eventSec']))
            | "Adjust EventSec" >> beam.Map(lambda event: {**event, "eventSec": event["eventSec"] + 3600 if event.get("matchPeriod") == "2H" else event["eventSec"]})
            | "Enrich with Match Info" >> beam.ParDo(EnrichWithMatchInfoFn(), matches_side_input)
            | "Group by matchId" >> beam.GroupByKey()
            | "Calculate Match Stats" >> beam.ParDo(MatchStatsFn())
            | "Write to BigQuery" >> WriteToBigQuery(
                table=OUTPUT_TABLE,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
