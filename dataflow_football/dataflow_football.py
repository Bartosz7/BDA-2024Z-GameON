def run():
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
    from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
    from apache_beam.coders import PickleCoder
    from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

    from collections import deque
    import json
    import logging
    import time
    import ast

    PROJECT_ID = 'bda-gameon-demo'
    DATASET_ID = 'football'
    TOPIC_ID = f"projects/{PROJECT_ID}/topics/football-events-topic"
    OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET_ID}.processed_data"
    MATCHES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.matches_info"

    logging.basicConfig(level=logging.INFO)

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = "DataflowRunner"

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = 'europe-central2'
    gcp_options.job_name = 'football-dataflow-enrichment-pipeline-42'
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
            event = json.loads(json_data.decode("utf-8"))
            if not isinstance(event, dict):
                raise ValueError("Parsed data is not a dictionary")
            return event
        except Exception as e:
            logging.error(f"Failed to parse JSON: {e} | Data: {json_data}")
            return None
    

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
                    },
                    "rolling_window": deque(),
                    "rolling_stats": {
                        "home_shots_last_5min": 0, "away_shots_last_5min": 0,
                        "home_passes_last_5min": 0, "away_passes_last_5min": 0,
                        "home_goals_last_5min": 0, "away_goals_last_5min": 0,
                        "home_fouls_last_5min": 0, "away_fouls_last_5min": 0,
                    },
                }

                cumulative_stats = stats["cumulative"]
                rolling_window = stats["rolling_window"]
                rolling_stats = stats["rolling_stats"]

                current_time = time.time()
                side = event.get("side", "unknown")
                event_name = event["eventName"].strip().lower()
                tags_list = event["tagsList"]

                if event_name == "shot":
                    if side == "home":
                        cumulative_stats["home_shots"] += 1
                        if 1801 in tags_list:
                            cumulative_stats["home_accurate_shots"] += 1
                    elif side == "away":
                        cumulative_stats["away_shots"] += 1
                        if 1801 in tags_list:
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
                elif "goal" in tags_list:
                    if side == "home" and 1801 in tags_list:
                        cumulative_stats["home_goals"] += 1
                    elif side == "away" and 1801 in tags_list:
                        cumulative_stats["away_goals"] += 1

                rolling_window.append((current_time, event))

                rolling_window = deque([(t, e) for t, e in rolling_window if current_time - t <= 300])

                self._update_rolling_stats(rolling_stats, event, remove=False)

                stats["rolling_window"] = rolling_window
                match_stats_state.write(stats)

                output = {
                    "matchId": match_id,
                    "eventSec": event["eventSec"],
                    **cumulative_stats,
                    **rolling_stats,
                }

                logging.info(f"Generated output: {output}")
                yield output
            except Exception as e:
                logging.error(f"Failed to process event: {e} | Event: {event}")
                raise

        def _update_rolling_stats(self, rolling_stats, event, remove=False):
            """
            Updates rolling stats incrementally by adding or removing an event.
            """
            factor = -1 if remove else 1
            side = event.get("side", "unknown")
            event_name = event["eventName"].strip().lower()
            tags_list = event["tagsList"]

            if event_name == "shot":
                if side == "home":
                    rolling_stats["home_shots_last_5min"] += factor
                elif side == "away":
                    rolling_stats["away_shots_last_5min"] += factor
            elif event_name == "pass":
                if side == "home":
                    rolling_stats["home_passes_last_5min"] += factor
                elif side == "away":
                    rolling_stats["away_passes_last_5min"] += factor
            elif event_name == "foul":
                if side == "home":
                    rolling_stats["home_fouls_last_5min"] += factor
                elif side == "away":
                    rolling_stats["away_fouls_last_5min"] += factor
            elif "goal" in tags_list:
                if side == "home":
                    rolling_stats["home_goals_last_5min"] += factor
                elif side == "away":
                    rolling_stats["away_goals_last_5min"] += factor


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
            | "Log Matches" >> beam.Map(lambda match: (logging.info(f"Match Key-Value: {match}"), match)[1])
        )

        matches_side_input = beam.pvalue.AsDict(matches)

        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=TOPIC_ID)
            | "Log Raw Pub/Sub Messages" >> beam.Map(lambda msg: logging.info(f"Raw Message: {msg}") or msg)
            | "Parse JSON" >> beam.Map(parse_event_data)
            | "Log Parsed Events" >> beam.Map(lambda event: logging.info(f"Parsed Event: {event}") or event)
            | "Filter Non-Null Events" >> beam.Filter(lambda event: event is not None)
            | "Inspect Event Before Stats" >> beam.Map(lambda event: logging.info(f"Event Before Stats: {type(event)} - {event}") or event)
            | "Adjust EventSec for 2H" >> beam.Map(
                lambda event: {
                    **event,
                    'eventSec': event['eventSec'] + 3600 if event.get('matchPeriod') == '2H' else event['eventSec']
                }
            )
            | "Log Adjusted Events" >> beam.Map(lambda event: logging.info(f"Adjusted Event: {event}") or event)
            | "Ensure tagsList Is List" >> beam.Map(ensure_tags_list)
            | "Enrich with Match Info" >> beam.ParDo(EnrichWithMatchInfoFn(), matches_side_input)
            | "Log Enriched Events" >> beam.Map(lambda event: logging.info(f"Enriched Event: {event}") or event)
            | "Key by matchId" >> beam.Map(lambda event: (event['matchId'], event))
            | "Calculate Match Stats" >> beam.ParDo(MatchStatsFn())
            | "Log Match Stats Output" >> beam.Map(lambda event: logging.info(f"Match Stats Output: {event}") or event)
            | "Write to BigQuery" >> WriteToBigQuery(
                table=OUTPUT_TABLE,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                custom_gcs_temp_location=gcp_options.temp_location,
            )
        )


if __name__ == "__main__":
    run()