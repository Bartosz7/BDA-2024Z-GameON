"""
pubsub_f1.py

Defines a Dataflow pipeline that reads messages from a Pub/Sub topic, parses
and preprocesses the JSON data, and writes it to BigQuery for further use.
"""
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import (PipelineOptions, StandardOptions, GoogleCloudOptions)

# Define your project and Pub/Sub topic information
PROJECT_ID = 'bda-gameon-demo'
TOPIC_ID = f'projects/{PROJECT_ID}/topics/formula-1-topic'
OUTPUT_TABLE = f'{PROJECT_ID}:post_data_test.posts'  # Replace with your dataset and table name
DATASET_ID = "f1"


def parse_and_flatten(json_data):
    """
    Extracts and flattens the nested JSON structure into rows.
    """
    parsed_data = json.loads(json_data.decode('utf-8'))
    races = parsed_data.get('races', {})

    for race_id, race_details in races.items():
        laps = race_details.get('laps', {})
        for lap_id, lap_details in laps.items():
            drivers = lap_details.get('drivers', {})
            for driver_id, driver_details in drivers.items():
                yield {
                    "raceId": int(race_id),
                    "driverId": int(driver_id),
                    "lap": int(lap_id),
                    "position": driver_details.get('position'),
                    "time": driver_details.get('time'),
                    "miliseconds": driver_details.get('milliseconds')
                }


# Define the pipeline
def run():
    # Set the pipeline options
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True  # Enables streaming mode

    # Set Google Cloud specific options
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = 'bda-gameon-demo'  # Your GCP project ID
    gcp_options.region = 'us-central1'  # Replace with your desired region
    gcp_options.job_name = 'f1-dataflow-pipeline-1'  # Unique job name
    gcp_options.staging_location = 'gs://gameon-bucket-1/staging'  # Replace with your GCS bucket
    gcp_options.temp_location = 'gs://gameon-bucket-1/temp'  # Replace with your GCS bucket

    with beam.Pipeline(options=options) as p:
        # Laps data pipeline
        (
            p
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=TOPIC_ID)
            | 'Parse JSON' >> beam.FlatMap(parse_and_flatten)
            | 'Write to BigQuery laps Table' >> beam.io.WriteToBigQuery(
                f'{PROJECT_ID}:{DATASET_ID}.laps',
                schema='raceId:INTEGER, driverId:INTEGER, lap:INTEGER, position:INTEGER, time:STRING, milliseconds:INTEGER',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

    p.run()


if __name__ == '__main__':
    run()
