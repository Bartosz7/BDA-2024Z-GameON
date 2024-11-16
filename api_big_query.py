import pandas as pd
import numpy as np
from google.cloud import bigquery

def main():
    project_id = "bda-gameon-demo"
    dataset_id = "f1"
    table_id = "drivers"
    dataset_ref = f"{project_id}.{dataset_id}"

    client = bigquery.Client(project=project_id)

    try:
        client.get_dataset(dataset_ref)
    except Exception as e:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset, exists_ok=True)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    csv_file_path = "data/drivers.csv"
    with open(csv_file_path, "rb") as file:
        load_job = client.load_table_from_file(
            file, table_ref, job_config=job_config
        )

    load_job.result()
    table = client.get_table(table_ref)
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_ref}"
    )

if __name__ == "__main__":
    main()