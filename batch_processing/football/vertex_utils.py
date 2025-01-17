import pandas as pd
from google.cloud import bigquery
import ast
import requests

def prepare_df_from_events_api(data):
    rows = [
        {
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
            'pos_orig_x': event_data.get('positions', {}).get('origin', {}).get('x'),
            'pos_orig_y': event_data.get('positions', {}).get('origin', {}).get('y'),
            'pos_dest_x': event_data.get('positions', {}).get('destination', {}).get('x'),
            'pos_dest_y': event_data.get('positions', {}).get('destination', {}).get('y'),
            'tags_list': event_data.get('tagsList', []),
        }
        for match_id, match_data in data['matches'].items()
        for event_id, event_data in match_data['events'].items()
    ]
    return pd.DataFrame(rows)

def fetch_events_data(matches_df, api_base_url):
    events_data = {"matches": {}}
    
    match_ids = matches_df['match_id'].unique()
    
    for match_id in match_ids:
        api_events_url = f"{api_base_url}/events/{match_id}"

        print(f"Fetching data from URL: {api_events_url}")
        
        response_events = requests.get(api_events_url)

        if response_events.status_code != 200:
            raise ValueError(f"Invalid response status code: {response_events.status_code}")
        
        events_response = response_events.json()
        
        if "matches" not in events_response or str(match_id) not in events_response["matches"]:
            raise ValueError(f"Invalid events data format for match ID {match_id}.")
        
        events_data["matches"][str(match_id)] = {
            "events": events_response["matches"][str(match_id)]["events"]
        }
    
    return events_data


def prepare_df_from_matches_api(data):
    rows = [
        {
            'match_id': match_id,
            **{
                key: match_data.get(key, '')
                for key in ['roundId', 'gameweek', 'teamsData', 'seasonId', 'dateutc', 'venue', 'date',
                            'referees', 'competitionId', 'duration', 'label', 'status', 'winner']
            },
            **{f"team1_{key}": match_data['team1'].get(key, '') for key in 
                ['scoreET', 'coachId', 'side', 'teamId', 'score', 'scoreP', 'hasFormation',
                 'formation', 'scoreHT', 'formation.bench', 'formation.lineup', 'formation.substitutions']},
            **{f"team2_{key}": match_data['team2'].get(key, '') for key in 
                ['scoreET', 'coachId', 'side', 'teamId', 'score', 'scoreP', 'hasFormation',
                 'formation', 'scoreHT', 'formation.bench', 'formation.lineup', 'formation.substitutions']}
        }
        for match_id, match_data in data['matches'].items()
    ]
    return pd.DataFrame(rows)


def adjust_event_sec(group):
    max_first_half_time = group[group['match_period'] == '1H']['event_sec'].max()
    group.loc[group['match_period'] == '2H', 'event_sec'] += max_first_half_time
    return group


def merge_events_and_matches(events_df, matches_df):
    matches_df = matches_df.astype({'team1_teamId': int, 'team2_teamId': int, 'winner': int})
    matches_df['result'] = matches_df.apply(
        lambda x: x['team1_side'] if x['team1_teamId'] == x['winner']
        else x['team2_side'] if x['team2_teamId'] == x['winner']
        else 'draw',
        axis=1
    )
    df = events_df.merge(matches_df, on='match_id')
    df['side'] = df['team_id'].map(
        matches_df.set_index('team1_teamId')['team1_side'].to_dict()
    ).fillna(
        df['team_id'].map(matches_df.set_index('team2_teamId')['team2_side'].to_dict())
    )
    df = df.groupby('match_id', group_keys=False).apply(adjust_event_sec)
    return df[["event_id", "match_id", "event_sec", "side", "event_name", "tags_list", "pos_orig_x", "result"]]


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

    df["tags_list"] = df["tags_list"].apply(safe_literal_eval)
    df["tags_labels"] = df["tags_list"].apply(lambda tags: [tag_to_label.get(tag, "Unknown") for tag in tags])
    df.drop(columns=["tags_list"], inplace=True)

    return df


def calculate_possession_time(events):
    """
    Calculate possession time for each team based on event timings.
    """
    events = events.sort_values(by=["match_id", "event_sec"]).copy()
    events["time_diff"] = events.groupby("match_id")["event_sec"].diff().fillna(0)
    
    events["home_possession_time"] = events.apply(
        lambda x: x["time_diff"] if x["side"] == "home" else 0, axis=1
    )
    events["away_possession_time"] = events.apply(
        lambda x: x["time_diff"] if x["side"] == "away" else 0, axis=1
    )
    return events


def prepare_aggregations(events):
    events["is_shot"] = (events["event_name"] == "Shot")
    events["is_accurate_shot"] = events["is_shot"] & events["tags_labels"].apply(lambda x: "accurate" in x)
    events["is_pass"] = (events["event_name"] == "Pass")
    events["is_goal"] = events["is_shot"] & events["tags_labels"].apply(lambda x: ("Goal" in x) & ("accurate" in x))
    events["is_foul"] = (events["event_name"] == "Foul")
    events["is_duel_won"] = (events["event_name"] == "Duel") & events["tags_labels"].apply(lambda x: "won" in x)
    
    events = calculate_possession_time(events)
    
    aggregations = []
    
    for match_id in events["match_id"].unique():
        match_events = events[events["match_id"] == match_id]
        for event_sec in sorted(match_events["event_sec"].unique()):
            current_events = match_events[match_events["event_sec"] <= event_sec]
            
            cumulative_stats = {
                "match_id": match_id,
                "event_sec": event_sec,
                "result": current_events["result"].iloc[-1],
                "home_shots": current_events[(current_events["side"] == "home") & current_events["is_shot"]].shape[0],
                "away_shots": current_events[(current_events["side"] == "away") & current_events["is_shot"]].shape[0],
                "home_accurate_shots": current_events[(current_events["side"] == "home") & current_events["is_accurate_shot"]].shape[0],
                "away_accurate_shots": current_events[(current_events["side"] == "away") & current_events["is_accurate_shot"]].shape[0],
                "home_passes": current_events[(current_events["side"] == "home") & current_events["is_pass"]].shape[0],
                "away_passes": current_events[(current_events["side"] == "away") & current_events["is_pass"]].shape[0],
                "home_goals": current_events[(current_events["side"] == "home") & current_events["is_goal"]].shape[0],
                "away_goals": current_events[(current_events["side"] == "away") & current_events["is_goal"]].shape[0],
                "home_fouls": current_events[(current_events["side"] == "home") & current_events["is_foul"]].shape[0],
                "away_fouls": current_events[(current_events["side"] == "away") & current_events["is_foul"]].shape[0],
                "home_duels_won": current_events[(current_events["side"] == "home") & current_events["is_duel_won"]].shape[0],
                "away_duels_won": current_events[(current_events["side"] == "away") & current_events["is_duel_won"]].shape[0],
                "home_possession_time": current_events["home_possession_time"].sum(),
                "away_possession_time": current_events["away_possession_time"].sum(),
            }
            
            diff_stats = {
                "shots_diff": cumulative_stats["home_shots"] - cumulative_stats["away_shots"],
                "accurate_shots_diff": cumulative_stats["home_accurate_shots"] - cumulative_stats["away_accurate_shots"],
                "passes_diff": cumulative_stats["home_passes"] - cumulative_stats["away_passes"],
                "goals_diff": cumulative_stats["home_goals"] - cumulative_stats["away_goals"],
                "fouls_diff": cumulative_stats["home_fouls"] - cumulative_stats["away_fouls"],
                "duels_won_diff": cumulative_stats["home_duels_won"] - cumulative_stats["away_duels_won"],
                "possession_time_diff": cumulative_stats["home_possession_time"] - cumulative_stats["away_possession_time"],
            }
            
            cumulative_stats.update(diff_stats)
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