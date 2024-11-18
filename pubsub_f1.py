import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Define your project and Pub/Sub topic information
PROJECT_ID = 'bda-gameon-demo'
TOPIC_ID = f'projects/{PROJECT_ID}/topics/formula-1-topic'
OUTPUT_TABLE = f'{PROJECT_ID}:post_data_test.posts'  # Replace with your dataset and table name


# Function to parse and transform the JSON data from Pub/Sub message
def parse_pubsub_message(message):
    # Decode the message
    row = json.loads(message.decode('utf-8'))
    return {
        'userId': row.get('userId'),
        'id': row.get('id'),
        'title': row.get('title'),
        'body': row.get('body')
    }


# Define the pipeline
def run():
    # Set the pipeline options
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True  # Enables streaming mode

    # Create the pipeline
    p = beam.Pipeline(options=options)

    (p 
     | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=TOPIC_ID)
     | 'Parse JSON' >> beam.Map(parse_pubsub_message)
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         OUTPUT_TABLE,
         schema='userId:INTEGER, id:INTEGER, title:STRING, body:STRING',
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

    # Run the pipeline
    p.run()#.wait_until_finish()


if __name__ == '__main__':
    run()
