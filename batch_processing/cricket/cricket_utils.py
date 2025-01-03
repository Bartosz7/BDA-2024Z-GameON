import pandas as pd
import requests
from google.cloud import bigquery
from typing import List

API_BASE_URL = "https://big-data-project-api-248863766350.europe-west3.run.app/cricket"
PROJECT_ID = "bda-gameon-demo"
DATASET_ID = "cricket"
TABLE_ID = "historic_data"
SCHEMA = [
    bigquery.SchemaField("inning", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("over", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("batter", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("bowler", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("non_striker", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("runs_batter", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("runs_extras", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("runs_total", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("cumulative_score", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("cumulative_wickets", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("winner", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("toss_decision", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("toss_winner", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("season", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("current_team", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_inning_total_score", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_inning_total_wickets", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("first_inning_run_rate", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("team_1", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("team_2", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("run_rate", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("required_run_rate", "FLOAT", mode="REQUIRED"),
]


def process_delivery(
    delivery: dict,
    match_id: int,
    inning: int,
    over: int,
    cumulative_score: int,
    cumulative_wickets: int,
) -> dict:
    """Process a single delivery and return the relevant data."""
    batter = delivery["batter"]
    bowler = delivery["bowler"]
    non_striker = delivery["non_striker"]
    runs_batter = delivery["runs"]["batter"]
    runs_extras = delivery["runs"]["extras"]
    runs_total = delivery["runs"]["total"]

    cumulative_score += runs_total
    if "wickets" in delivery:
        cumulative_wickets += len(delivery["wickets"])

    return (
        {
            "match_id": match_id,
            "inning": inning,
            "over": over,
            "batter": batter,
            "bowler": bowler,
            "non_striker": non_striker,
            "runs_batter": runs_batter,
            "runs_extras": runs_extras,
            "runs_total": runs_total,
            "cumulative_score": cumulative_score,
            "cumulative_wickets": cumulative_wickets,
        },
        cumulative_score,
        cumulative_wickets,
    )


def process_players(data: dict, first_team: str) -> dict:
    """Process the players data and return a dictionary."""
    teams = {}
    for team, players in data["players"].items():
        teams[team] = (
            players[:11]
            if len(players) >= 11
            else players + [f"Placeholder {i}" for i in range(11 - len(players))]
        )

    player_dict = {}

    team_1_players = teams[first_team]
    team_2 = [team for team in teams if team != first_team][0]
    team_2_players = teams[team_2]

    player_dict["team_1"] = first_team
    player_dict["team_2"] = team_2
    for i, player in enumerate(team_1_players, start=1):
        player_dict[f"team_1_player_{i}"] = player
    for i, player in enumerate(team_2_players, start=1):
        player_dict[f"team_2_player_{i}"] = player

    return player_dict


def process_match(match_id: int) -> pd.DataFrame:
    """Process the match data and return a DataFrame."""
    rows = []
    cumulative_score = {1: 0, 2: 0}
    cumulative_wickets = {1: 0, 2: 0}
    first_inning_summary = {}

    for inning in [1, 2]:
        for over in range(20):
            api_url = f"{API_BASE_URL}/{match_id}/{inning}/{over}"
            response = requests.get(api_url)
            data = response.json()

            if data == "Invalid data provided.":
                break

            if inning == 1 and over == 0:
                teams = process_players(data, first_team=data.get("team"))

            if inning == 1:
                for delivery in data.get("deliveries", []):
                    row, cumulative_score[inning], cumulative_wickets[inning] = (
                        process_delivery(
                            delivery,
                            match_id,
                            inning,
                            over,
                            cumulative_score[inning],
                            cumulative_wickets[inning],
                        )
                    )
            else:
                for delivery in data.get("deliveries", []):
                    row, cumulative_score[inning], cumulative_wickets[inning] = (
                        process_delivery(
                            delivery,
                            match_id,
                            inning,
                            over,
                            cumulative_score[inning],
                            cumulative_wickets[inning],
                        )
                    )

            row.update(
                {
                    "winner": data.get("winner"),
                    "toss_decision": data.get("toss_decision"),
                    "toss_winner": data.get("toss_winner"),
                    "season": data.get("season"),
                    "city": data.get("city"),
                    "current_team": data.get("team"),
                    "first_inning_total_score": first_inning_summary.get(
                        "total_score", 0
                    ),
                    "first_inning_total_wickets": first_inning_summary.get(
                        "total_wickets", 0
                    ),
                    "first_inning_run_rate": first_inning_summary.get("run_rate", 0),
                }
            )
            row.update(teams)
            rows.append(row)

        first_inning_summary = {
            "total_score": cumulative_score[1],
            "total_wickets": cumulative_wickets[1],
            "run_rate": cumulative_score[1] / (over + 1),
        }

    df = pd.DataFrame(rows)
    return df


def load_data(match_ids: List[int]) -> pd.DataFrame:
    """Load the cricket data for the given match IDs."""
    all_matches = []
    for match_id in match_ids:
        match_data = process_match(match_id)
        all_matches.append(match_data)

    full_df = pd.concat(all_matches, ignore_index=True)
    return full_df


def transform_season(season: str) -> float:
    """Transform the season string to single number."""
    if isinstance(season, int):
        return season
    if isinstance(season, str):
        parts = season.split("/")
        if len(parts) == 1:
            return int(parts[0])
        if len(parts) == 2:
            try:
                return (int(parts[0]) + int(parts[1])) / 2
            except ValueError:
                raise ValueError("Invalid season format.")


def transform_cricket_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the cricket data to the required format."""
    df["winner"] = df.apply(
        lambda row: 1 if row["winner"] == row["team_2"] else 0, axis=1
    )
    df["toss_decision"] = df.apply(
        lambda row: 1 if row["toss_decision"] == "field" else 0, axis=1
    )
    df["toss_winner"] = df.apply(
        lambda row: 1 if row["toss_winner"] == row["team_2"] else 0, axis=1
    )
    df["current_team"] = df.apply(
        lambda row: 1 if row["current_team"] == row["team_2"] else 0, axis=1
    )
    df["season"] = df["season"].apply(transform_season)
    df["run_rate"] = df["cumulative_score"] / (df["over"] + 1)
    df["required_run_rate"] = df.apply(
        lambda row: (
            (row["first_inning_total_score"] - row["cumulative_score"])
            / max(20 - row["over"] - 1, 1)
            if row["inning"] == 2
            else 0
        ),
        axis=1,
    )

    columns_to_drop = ["match_id", "city"]
    df = df.drop(columns=[col for col in columns_to_drop if col in df])

    return df


def save_historic_to_big_query(
    aggregations: pd.DataFrame, write_disp: str = "APPEND"
) -> None:
    """Save the aggregated data to BigQuery."""
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    table_ref = f"{dataset_ref}.{TABLE_ID}"
    client = bigquery.Client(project=PROJECT_ID)

    try:
        client.get_dataset(dataset_ref)
    except Exception as e:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {DATASET_ID} created.")

    assert write_disp in ["APPEND", "TRUNCATE"]

    if write_disp == "APPEND":
        write_disp = bigquery.WriteDisposition.WRITE_APPEND
    elif write_disp == "TRUNCATE":
        write_disp = bigquery.WriteDisposition.WRITE_TRUNCATE

    try:
        client.get_table(table_ref)
    except Exception as e:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        client.create_table(table)
        print(f"Table {TABLE_ID} created with NOT NULL schema.")

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disp,
    )

    load_job = client.load_table_from_dataframe(
        aggregations, table_ref, job_config=job_config
    )
    load_job.result()
