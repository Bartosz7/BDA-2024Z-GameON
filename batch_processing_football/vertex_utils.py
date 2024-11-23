import pandas as pd
import numpy as np
from google.cloud import bigquery
import ast
import requests

def prepare_df_from_events_api(data):
    rows = []
    for match_id, match_data in data['matches'].items():
        for event_id, event_data in match_data['events'].items():
            rows.append({
                'match_id': match_id,
                'event_id': event_id,
                'event_name': event_data.get('eventName', ''),
                'sub_event_name': event_data.get('subEventName', ''),
                'tags': event_data.get('tags', []),
                'player_id': event_data.get('playerId', ''),
                'team_id': event_data.get('teamId', ''),
                'match_period': event_data.get('matchPeriod', ''),
                'event_sec': event_data.get('eventSec', 0),
                'sub_event_id': event_data.get('subEventId', ''),
                'origin_x': event_data.get('positions', {}).get('origin', {}).get('x', None),
                'origin_y': event_data.get('positions', {}).get('origin', {}).get('y', None),
                'destination_x': event_data.get('positions', {}).get('destination', {}).get('x', None),
                'destination_y': event_data.get('positions', {}).get('destination', {}).get('y', None),
                'tags_list': event_data.get('tagsList', []),
            })

    return pd.DataFrame(rows)

def fetch_events_data(matches_data, api_base_url):
    events_data = {"matches": {}}
    for match_id in matches_data["matches"].keys():
        api_events_url = f"{api_base_url}/events/{match_id}"

        response_events = requests.get(api_events_url)
        events_response = response_events.json()

        if "matches" not in events_response or str(match_id) not in events_response["matches"]:
            raise ValueError(f"Invalid events data format for match ID {match_id}.")

        events_data["matches"][str(match_id)] = {
            "events": events_response["matches"][str(match_id)]["events"]
        }

    return events_data


def prepare_df_from_matches_api(data):
    rows = []
    for match_id, match_data in data['matches'].items():
        match_info = match_data
        row = {
            'match_id': match_id,
            'round_id': match_info.get('roundId', ''),
            'gameweek': match_info.get('gameweek', ''),
            'teams_data': match_info.get('teamsData', ''),
            'season_id': match_info.get('seasonId', ''),
            'dateutc': match_info.get('dateutc', ''),
            'venue': match_info.get('venue', ''),
            'date': match_info.get('date', ''),
            'referees': match_info.get('referees', ''),
            'competition_id': match_info.get('competitionId', ''),

            # Team 1 details
            'team1_score_et': match_info['team1'].get('scoreET', ''),
            'team1_coach_id': match_info['team1'].get('coachId', ''),
            'team1_side': match_info['team1'].get('side', ''),
            'team1_id': match_info['team1'].get('teamId', ''),
            'team1_score': match_info['team1'].get('score', ''),
            'team1_score_p': match_info['team1'].get('scoreP', ''),
            'team1_has_formation': match_info['team1'].get('hasFormation', ''),
            'team1_formation': match_info['team1'].get('formation', ''),
            'team1_score_ht': match_info['team1'].get('scoreHT', ''),
            'team1_formation_bench': match_info['team1'].get('formation.bench', ''),
            'team1_formation_lineup': match_info['team1'].get('formation.lineup', ''),
            'team1_formation_substitutions': match_info['team1'].get('formation.substitutions', ''),

            # Team 2 details
            'team2_score_et': match_info['team2'].get('scoreET', ''),
            'team2_coach_id': match_info['team2'].get('coachId', ''),
            'team2_side': match_info['team2'].get('side', ''),
            'team2_id': match_info['team2'].get('teamId', ''),
            'team2_score': match_info['team2'].get('score', ''),
            'team2_score_p': match_info['team2'].get('scoreP', ''),
            'team2_has_formation': match_info['team2'].get('hasFormation', ''),
            'team2_formation': match_info['team2'].get('formation', ''),
            'team2_score_ht': match_info['team2'].get('scoreHT', ''),
            'team2_formation_bench': match_info['team2'].get('formation.bench', ''),
            'team2_formation_lineup': match_info['team2'].get('formation.lineup', ''),
            'team2_formation_substitutions': match_info['team2'].get('formation.substitutions', ''),
        }
        rows.append(row)

    return pd.DataFrame(rows)


def adjust_event_sec(group):
    max_first_half_time = group[group['matchPeriod'] == '1H']['eventSec'].max()
    group.loc[group['matchPeriod'] == '2H', 'eventSec'] += max_first_half_time
    return group


def merge_events_and_matches(events_df, matches_df):
    matches_df = matches_df[['match_id', 'team1_id', 'team2_id', 'team1_side', 'team2_side']]
    matches_df['winner'] = matches_df.apply(lambda x: x['team1_side'] if x['team1_id'] == x['winner'] else x['team2_side'] if x['team2_id'] == x['winner'] else 'draw', axis=1)
    df = pd.merge(events_df, matches_df, left_on='match_id', right_on='match_id')
    df['side' ] = df.apply(lambda x: x['team1_side'] if x['team1_id'] == x['team_id'] else x['team2_side'], axis=1)
    columns_used = [
        "event_id",
        "matchId",
        "eventSec",
        "side",
        "eventName",
        "tagsLabels",
        "pos_orig_x"
    ]
    df = df[columns_used]
    df = df.groupby('match_id', group_keys=False).apply(adjust_event_sec)
    return df

def safe_literal_eval(val):
    try:
        if pd.isnull(val):
            return []
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return []

def enrich_with_tags_names(df):
    project_id = "bda-gameon-demo"
    dataset_id = "football"
    table_id = "tags2name"

    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT *
        FROM `{dataset_id}.{table_id}`
    """

    query_job = client.query(query)
    df_tags = query_job.to_dataframe()
    tag_to_label = df_tags.set_index("Tag")["Label"].to_dict()

    df["tagsList"] = df["tagsList"].apply(safe_literal_eval)
    df["tagsLabels"] = df["tagsList"].apply(lambda tags: [tag_to_label.get(tag, "Unknown") for tag in tags])
    df.drop(columns=["tagsList"], inplace=True)

    return df


def prepare_aggregations(events):
    aggregations = []

    for _, row in events.iterrows():
        match_id = row["matchId"]
        event_sec = row["eventSec"]

        # Filter events up to the current event_sec
        match_events = events[(events["matchId"] == match_id) & (events["eventSec"] <= event_sec)]

        # Cumulative features
        cumulative_stats = {
            "matchId": match_id,
            "eventSec": event_sec,
            "home_shots": match_events[(match_events["side"] == "home") & (match_events["eventName"] == "Shot")].shape[0],
            "away_shots": match_events[(match_events["side"] == "away") & (match_events["eventName"] == "Shot")].shape[0],
            "home_accurate_shots": match_events[
                (match_events["side"] == "home") &
                (match_events["eventName"] == "Shot") &
                (match_events["tagsLabels"].apply(lambda x: "accurate" in x))
            ].shape[0],
            "away_accurate_shots": match_events[
                (match_events["side"] == "away") &
                (match_events["eventName"] == "Shot") &
                (match_events["tagsLabels"].apply(lambda x: "accurate" in x))
            ].shape[0],
            "home_passes": match_events[(match_events["side"] == "home") & (match_events["eventName"] == "Pass")].shape[0],
            "away_passes": match_events[(match_events["side"] == "away") & (match_events["eventName"] == "Pass")].shape[0],
            "home_goals": match_events[
                (match_events["side"] == "home") &
                (match_events["tagsLabels"].apply(lambda x: ("Goal" in x) & ("accurate" in x)))
            ].shape[0],
            "away_goals": match_events[
                (match_events["side"] == "away") &
                (match_events["tagsLabels"].apply(lambda x: ("Goal" in x) & ("accurate" in x)))
            ].shape[0],
            "home_avg_pos_x": match_events[match_events["side"] == "home"]["pos_orig_x"].mean(),
            "away_avg_pos_x": match_events[match_events["side"] == "away"]["pos_orig_x"].mean(),
            "home_fouls": match_events[(match_events["side"] == "home") & (match_events["eventName"] == "Foul")].shape[0],
            "away_fouls": match_events[(match_events["side"] == "away") & (match_events["eventName"] == "Foul")].shape[0],
            "home_yellow_cards": match_events[
                (match_events["side"] == "home") & (match_events["eventName"] == "Card") &
                (match_events["tagsLabels"].apply(lambda x: "yellow_card" in x))
            ].shape[0],
            "away_yellow_cards": match_events[
                (match_events["side"] == "away") & (match_events["eventName"] == "Card") &
                (match_events["tagsLabels"].apply(lambda x: "yellow_card" in x))
            ].shape[0],
            "home_red_cards": match_events[
                (match_events["side"] == "home") & (match_events["eventName"] == "Card") &
                (match_events["tagsLabels"].apply(lambda x: "red_card" in x))
            ].shape[0],
            "away_red_cards": match_events[
                (match_events["side"] == "away") & (match_events["eventName"] == "Card") &
                (match_events["tagsLabels"].apply(lambda x: "red_card" in x))
            ].shape[0],
        }

        # Rolling window features (last 5 minutes)
        rolling_window_start = max(0, event_sec - 300)
        rolling_events = match_events[match_events["eventSec"] >= rolling_window_start]
        rolling_stats = {
            "home_shots_last_5min": rolling_events[(rolling_events["side"] == "home") & (rolling_events["eventName"] == "Shot")].shape[0],
            "away_shots_last_5min": rolling_events[(rolling_events["side"] == "away") & (rolling_events["eventName"] == "Shot")].shape[0],
            "home_passes_last_5min": rolling_events[(rolling_events["side"] == "home") & (rolling_events["eventName"] == "Pass")].shape[0],
            "away_passes_last_5min": rolling_events[(rolling_events["side"] == "away") & (rolling_events["eventName"] == "Pass")].shape[0],
            "home_goals_last_5min": rolling_events[
                (rolling_events["side"] == "home") &
                (rolling_events["tagsLabels"].apply(lambda x: ("Goal" in x) & ("accurate" in x)))
            ].shape[0],
            "away_goals_last_5min": rolling_events[
                (rolling_events["side"] == "away") &
                (rolling_events["tagsLabels"].apply(lambda x: ("Goal" in x) & ("accurate" in x)))
            ].shape[0],
            "home_fouls_last_5min": rolling_events[(rolling_events["side"] == "home") & (rolling_events["eventName"] == "Foul")].shape[0],
            "away_fouls_last_5min": rolling_events[(rolling_events["side"] == "away") & (rolling_events["eventName"] == "Foul")].shape[0],
            "home_avg_pos_x_last_5min": rolling_events[rolling_events["side"] == "home"]["pos_orig_x"].mean(),
            "away_avg_pos_x_last_5min": rolling_events[rolling_events["side"] == "away"]["pos_orig_x"].mean(),
        }

        # Combine features
        cumulative_stats.update(rolling_stats)
        aggregations.append(cumulative_stats)

    return pd.DataFrame(aggregations)



def save_historic_to_big_query(aggregations, write_disp="APPEND"):
    project_id = "bda-gameon-demo"
    dataset_id = "football"
    table_id = "historic_aggregations"
    dataset_ref = f"{project_id}.{dataset_id}"

    client = bigquery.Client(project=project_id)

    try:
        client.get_dataset(dataset_ref)
    except Exception as e:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset, exists_ok=True)

    assert write_disp in ["APPEND", "TRUNCATE"]

    if write_disp == "APPEND":
        write_disp = bigquery.WriteDisposition.WRITE_APPEND
    elif write_disp == "TRUNCATE":
        write_disp = bigquery.WriteDisposition.WRITE_TRUNCATE

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disp,
    )

    load_job = client.load_table_from_dataframe(
        aggregations, table_ref, job_config=job_config
    )
    load_job.result()

def load_historic():
    project_id = "bda-gameon-demo"
    dataset_id = "football"
    table_id = "historic_aggregations"

    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT *
        FROM `{dataset_id}.{table_id}`
    """

    return client.query(query).to_dataframe()