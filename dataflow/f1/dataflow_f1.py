"""
dataflow_f1.py

Defines a Dataflow pipeline that reads messages from a Pub/Sub topic, parses
and preprocesses the JSON data, and writes it to BigQuery for further use.
"""
import json
import numpy as np
import apache_beam as beam
from google.cloud import bigquery
from apache_beam.options.pipeline_options import (PipelineOptions, StandardOptions, GoogleCloudOptions)

# Define your project and Pub/Sub topic information
PROJECT_ID = 'bda-gameon-demo'
TOPIC_ID = f'projects/{PROJECT_ID}/topics/formula-1-topic'
OUTPUT_TABLE = f'{PROJECT_ID}:post_data_test.posts'
DATASET_ID = "f1"


def process_lap_data(json_data):
    parsed_data = json.loads(json_data.decode('utf-8'))
    races = parsed_data.get("races", {})
    for race_id, race_data in races.items():
        laps = race_data.get("laps", {})
        for lap, lap_data in laps.items():
            drivers = lap_data.get("drivers", {})
            for driver_id, driver_data in drivers.items():
                yield {
                    "lap": int(lap),
                    "driver": int(driver_id),
                    "position": driver_data["position"],
                }

def calculate_statistics(element, position_history):
    import numpy as np
    driver = element["driver"]
    lap = element["lap"]
    position = element["position"]

    if driver not in position_history:
        position_history[driver] = []
    position_history[driver].append(position)

    mean_position = np.mean(position_history[driver])
    std_position = np.std(position_history[driver])
    min_position = np.min(position_history[driver])
    last_5_mean = np.mean(position_history[driver][-5:]) if len(position_history[driver]) >= 5 else mean_position

    element["mean_position_up_to_lap"] = float(mean_position)
    element["std_position_up_to_lap"] = float(std_position)
    element["min_position_up_to_lap"] = float(min_position)
    element["last_5_laps_mean_position"] = float(last_5_mean)

    return element

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = 'bda-gameon-demo'
    gcp_options.region = 'europe-west3'
    gcp_options.job_name = 'f1-dataflow-pipeline-3'
    gcp_options.staging_location = 'gs://gameon-bucket-1/staging'
    gcp_options.temp_location = 'gs://gameon-bucket-1/temp'

    with beam.Pipeline(options=options) as pipeline:
            position_history = beam.pvalue.AsSingleton(pipeline | "Create Empty History" >> beam.Create([{}]))

            (
                pipeline
                | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=TOPIC_ID)
                | "Extract Lap Data" >> beam.FlatMap(process_lap_data)
                | "Calculate Statistics" >> beam.Map(calculate_statistics, position_history=position_history)
                | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    f'{PROJECT_ID}:{DATASET_ID}.stream_laps',
                    schema=(
                        "lap:INTEGER, driver:INTEGER, position:INTEGER, "
                        "mean_position_up_to_lap:FLOAT, std_position_up_to_lap:FLOAT, "
                        "min_position_up_to_lap:FLOAT, last_5_laps_mean_position:FLOAT"
                    ),
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
            )

    pipeline.run()


if __name__ == '__main__':
    run()
