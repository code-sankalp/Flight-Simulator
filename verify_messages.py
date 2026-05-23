import json
import os

from google.cloud import pubsub_v1

PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "flight-events-sub")


def callback(message):
    try:
        event = json.loads(message.data.decode("utf-8"))
        print(
            f"  Received: {event.get('flight_id', '')} | "
            f"{event.get('origin', '')} -> {event.get('destination', '')} | "
            f"status={event.get('status', '')} | "
            f"delay={event.get('delay_minutes', 0)}m"
        )
        message.ack()
    except Exception as exc:
        print(f"Failed to decode message: {exc}")
        message.nack()


def main(timeout=60):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print(f"Listening on {subscription_path} ...\n")
    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull.result(timeout=timeout)
    except Exception:
        streaming_pull.cancel()
        print("\nDone listening.")


if __name__ == "__main__":
    main()
