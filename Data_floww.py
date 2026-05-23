import json
import os

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
SUBSCRIPTION = os.getenv(
    "PUBSUB_SUBSCRIPTION",
    "projects/your-gcp-project-id/subscriptions/flight-events-bq-sub",
)
BIGQUERY_TABLE = os.getenv(
    "BIGQUERY_TABLE",
    "your-gcp-project-id:flight_data.flight_events",
)
RUNNER = os.getenv("BEAM_RUNNER", "DirectRunner")

TABLE_SCHEMA = {
    "fields": [
        {"name": "flight_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "airline", "type": "STRING", "mode": "NULLABLE"},
        {"name": "origin", "type": "STRING", "mode": "NULLABLE"},
        {"name": "destination", "type": "STRING", "mode": "NULLABLE"},
        {"name": "scheduled_time", "type": "STRING", "mode": "NULLABLE"},
        {"name": "delay_minutes", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "delay_reason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_timestamp", "type": "STRING", "mode": "NULLABLE"},
    ]
}


def parse_delay_minutes(value):
    if value in (None, ""):
        return 0
    return int(value)


class ParseFlightEvent(beam.DoFn):
    def process(self, message):
        try:
            event = json.loads(message.decode("utf-8"))
            yield {
                "flight_id": event.get("flight_id", ""),
                "airline": event.get("airline", ""),
                "origin": event.get("origin", ""),
                "destination": event.get("destination", ""),
                "scheduled_time": event.get("scheduled_time", ""),
                "delay_minutes": parse_delay_minutes(event.get("delay_minutes", 0)),
                "status": event.get("status", ""),
                "delay_reason": event.get("delay_reason", "none"),
                "event_timestamp": event.get("event_timestamp", ""),
            }
        except Exception as exc:
            print(f"Failed to parse: {message!r} | Error: {exc}")


def build_options():
    return PipelineOptions(
        [
            f"--project={PROJECT_ID}",
            f"--runner={RUNNER}",
        ]
    )


def run():
    options = build_options()
    options.view_as(StandardOptions).streaming = True

    print(f"Apache Beam version : {beam.__version__}")
    print(f"Reading from        : {SUBSCRIPTION}")
    print(f"Writing to          : {BIGQUERY_TABLE}")
    print("Press Ctrl+C to stop\n")

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse JSON" >> beam.ParDo(ParseFlightEvent())
            | "Write to BigQuery"
            >> WriteToBigQuery(
                table=BIGQUERY_TABLE,
                schema=TABLE_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
