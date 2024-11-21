import pandas as pd
import numpy as np
from google.cloud import bigquery

def prepare_df_from_json(data):
    rows = []
    for race_id, race_data in data['races'].items():
        for lap, lap_data in race_data['laps'].items():
            for driver, driver_data in lap_data['drivers'].items():
                rows.append({
                    'race_id': race_id,
                    'lap': int(lap),
                    'driver': driver,
                    'position': driver_data['position'],
                    'milliseconds': driver_data['milliseconds']
                })

    return pd.DataFrame(rows)

def prepare_aggregations(df):
    df = df.sort_values(by=["race_id", "driver", "lap"])
    group = df.groupby(["race_id", "driver"])

    df["mean_position_up_to_lap"] = group["position"].expanding().mean().reset_index(level=[0, 1], drop=True)
    df["std_position_up_to_lap"] = group["position"].expanding().std().reset_index(level=[0, 1], drop=True)
    df["min_position_up_to_lap"] = group["position"].expanding().min().reset_index(level=[0, 1], drop=True)

    df["last_5_laps_mean_position"] = (
        group["position"]
        .rolling(window=5, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    )

    final_positions = group.apply(lambda x: x.loc[x["lap"].idxmax(), "position"])
    final_positions.name = "final_position"
    aggregations = df.merge(final_positions, on=["race_id", "driver"])

    aggregations = aggregations.dropna().reset_index(drop=True)

    aggregations.race_id = np.int32(aggregations.race_id)
    aggregations.driver = np.int32(aggregations.driver)

    return aggregations

def enrich_with_drivers(aggregations, curr_year):
    project_id = "bda-gameon-demo"
    dataset_id = "f1"
    table_id = "drivers"

    client = bigquery.Client(project=project_id)

    driver_id_list = np.unique(aggregations.driver)
    ids_str = ", ".join(f"{id}" for id in driver_id_list)

    query = f"""
        SELECT *
        FROM `{dataset_id}.{table_id}`
        WHERE driverId IN ({ids_str})
    """

    query_job = client.query(query)
    df = query_job.to_dataframe()[["driverId", "dob"]]

    df['age'] = df['dob'].apply(lambda x: curr_year - x.year)
    df_drivers = df[['driverId', 'age']].drop_duplicates().reset_index(drop=True)

    return pd.merge(aggregations, df_drivers, left_on="driver", right_on="driverId", how="left")

def enrich_with_races(aggregations):
    project_id = "bda-gameon-demo"
    dataset_id = "f1"
    table_id = "races"

    client = bigquery.Client(project=project_id)

    races_id_list = np.unique(aggregations.race_id)
    ids_str = ", ".join(f"{id}" for id in races_id_list)

    query = f"""
        SELECT *
        FROM `{dataset_id}.{table_id}`
        WHERE raceId IN ({ids_str})
    """

    query_job = client.query(query)
    df_races = query_job.to_dataframe()[["raceId", "circuitId"]]

    return pd.merge(aggregations, df_races, left_on="race_id", right_on="raceId", how="left")

def save_historic_to_big_query(aggregations, write_disp="APPEND"):
    project_id = "bda-gameon-demo"
    dataset_id = "f1"
    table_id = "historic_data"
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
    dataset_id = "f1"
    table_id = "historic_data"

    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT *
        FROM `{dataset_id}.{table_id}`
    """

    return client.query(query).to_dataframe()