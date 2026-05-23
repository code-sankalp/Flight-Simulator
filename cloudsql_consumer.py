import json
import os

import pymysql
from google.cloud import pubsub_v1

PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "flight-events-sub")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "flight_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "flight_db")

rows_inserted = 0


def get_connection():
    if not MYSQL_PASSWORD:
        raise RuntimeError("MYSQL_PASSWORD environment variable is required")

    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        autocommit=True,
        connect_timeout=10,
    )


def insert_row(event):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO flight_events
                (flight_id, airline, origin, destination, scheduled_time,
                 delay_minutes, status, delay_reason, event_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event.get("flight_id", ""),
                    event.get("airline", ""),
                    event.get("origin", ""),
                    event.get("destination", ""),
                    event.get("scheduled_time", ""),
                    int(event.get("delay_minutes", 0)),
                    event.get("status", ""),
                    event.get("delay_reason", "none"),
                    event.get("event_timestamp", ""),
                ),
            )


def callback(message):
    global rows_inserted
    try:
        event = json.loads(message.data.decode("utf-8"))
        insert_row(event)
        rows_inserted += 1
        print(
            f"[{rows_inserted:04d}] Inserted: {event.get('flight_id', '')} | "
            f"{event.get('origin', '')} -> {event.get('destination', '')} | "
            f"delay={event.get('delay_minutes', 0)}m | "
            f"status={event.get('status', '')}"
        )
        message.ack()
    except Exception as exc:
        print(f"Error: {exc}")
        message.nack()


def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print(f"Connecting to MySQL at {MYSQL_HOST}")
    print(f"Listening on {subscription_path}")
    print("Press Ctrl+C to stop\n")

    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull.result()
    except KeyboardInterrupt:
        streaming_pull.cancel()
        print(f"\nStopped. Total rows inserted: {rows_inserted}")


if __name__ == "__main__":
    main()
