"""
Defines a Dataflow pipeline to process cricket data from Pub/Sub messages (one over per message).
"""
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions


PROJECT_ID = 'bda-gameon-demo'
TOPIC_ID = f'projects/{PROJECT_ID}/topics/cricket-topic'
DATASET_ID = "cricket"
OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET_ID}.processed_data"


def transform_season(season):
    """Transform the season string to a single number."""
    parts = season.split("/")
    if len(parts) == 1:
        return int(parts[0])
    if len(parts) == 2:
        return (int(parts[0]) + int(parts[1])) / 2


def process_over(data, position_history):
    """
    Process a single over and calculate cumulative metrics using shared state.

    :param data: JSON data for the current over.
    :param position_history: Shared dictionary holding cumulative values.
    :return: List of processed records for each delivery in the over.
    """
    try:
        parsed_data = json.loads(data.decode("utf-8"))
        current_team = parsed_data["team"]
    except Exception as e:
        return []
    
    match_id = parsed_data["match_id"]
    teams = list(parsed_data["players"].keys())
    first_team = parsed_data["toss_winner"] if parsed_data["toss_decision"] == "field" else teams[1 - teams.index(parsed_data["toss_winner"])]
    second_team = teams[1 - teams.index(first_team)]
    inning = 1 + int(first_team == current_team)
    over_number = parsed_data["over_number"]
    deliveries = parsed_data["deliveries"]

    if match_id not in position_history:
        position_history[match_id] = {
            1: {"score": 0, "wickets": 0},
            2: {"score": 0, "wickets": 0},
        }

    cumulative_score = position_history[match_id][inning]["score"]
    cumulative_wickets = position_history[match_id][inning]["wickets"]

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
            "cumulative_wickets": cumulative_wickets,
            "toss_decision": 1 if parsed_data["toss_decision"] == "field" else 0,
            "toss_winner": 1 if parsed_data["toss_winner"] != first_team else 0,
            "season": transform_season(parsed_data["season"]),
            "current_team": 1 if current_team != first_team else 0,
            "first_inning_total_score": position_history[match_id][1]["score"] if inning == 2 else 0,
            "first_inning_total_wickets": position_history[match_id][1]["wickets"] if inning == 2 else 0,
            "first_inning_run_rate": position_history[match_id][1]["score"] / position_history[match_id][1]["wickets"] if inning == 2 and position_history[match_id][1]["wickets"] > 0 else 0,
            "team_1": first_team,
            "team_2": second_team,
            "run_rate": cumulative_score / (over_number + 1),
            "required_run_rate": (position_history[match_id][1]["score"] - cumulative_score) / max(20 - over_number - 1, 1) if inning == 2 else 0,
        }
        processed_rows.append(row)

    position_history[match_id][inning]["score"] = cumulative_score
    position_history[match_id][inning]["wickets"] = cumulative_wickets
    return [processed_rows[-1]]


def run():
    """Main function to execute the Dataflow pipeline."""
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = 'europe-west3'
    gcp_options.job_name = 'cricket-dataflow-pipeline-3'
    gcp_options.staging_location = 'gs://gameon-bucket-1/staging'
    gcp_options.temp_location = 'gs://gameon-bucket-1/temp'

    with beam.Pipeline(options=options) as pipeline:
        position_history = beam.pvalue.AsSingleton(pipeline | "Initialize History" >> beam.Create([{}]))

        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=TOPIC_ID)
            | "Process Over Data" >> beam.FlatMap(process_over, position_history=position_history)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema=(
                    "match_id:INTEGER, inning:INTEGER, over:INTEGER, batter:STRING, bowler:STRING, non_striker:STRING, "
                    "runs_batter:INTEGER, runs_extras:INTEGER, runs_total:INTEGER, cumulative_score:INTEGER, "
                    "cumulative_wickets:INTEGER, toss_decision:INTEGER, toss_winner:INTEGER, "
                    "season:FLOAT, current_team:INTEGER, first_inning_total_score:INTEGER, first_inning_total_wickets:INTEGER, "
                    "first_inning_run_rate:FLOAT, team_1:STRING, team_2:STRING, run_rate:FLOAT, required_run_rate:FLOAT"
                ),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
