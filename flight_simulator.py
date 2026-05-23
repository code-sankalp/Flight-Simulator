import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1

PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
TOPIC_ID = os.getenv("TOPIC_ID", "flight-events")

AIRLINES = ["IndiGo", "Air India", "SpiceJet", "Vistara", "GoFirst"]
AIRPORTS = ["DEL", "BOM", "BLR", "HYD", "MAA", "CCU", "PNQ", "AMD"]
DELAY_REASONS = ["weather", "crew", "technical", "air_traffic"]


def generate_flight_event():
    origin = random.choice(AIRPORTS)
    destination = random.choice([a for a in AIRPORTS if a != origin])
    scheduled = datetime.now(timezone.utc) + timedelta(hours=random.randint(1, 12))
    delay_mins = random.choices(
        [0, random.randint(15, 45), random.randint(60, 180)],
        weights=[60, 30, 10],
    )[0]
    reason = "none" if delay_mins == 0 else random.choice(DELAY_REASONS)

    return {
        "flight_id": f"{random.choice(['6E', 'AI', 'SG', 'UK', 'G8'])}{random.randint(100, 999)}",
        "airline": random.choice(AIRLINES),
        "origin": origin,
        "destination": destination,
        "scheduled_time": scheduled.isoformat(),
        "delay_minutes": delay_mins,
        "status": "ON_TIME" if delay_mins == 0 else "DELAYED",
        "delay_reason": reason,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def publish_event(event, publisher=None, topic_path=None):
    publisher = publisher or pubsub_v1.PublisherClient()
    topic_path = topic_path or publisher.topic_path(PROJECT_ID, TOPIC_ID)
    data = json.dumps(event).encode("utf-8")
    attributes = {
        "status": event["status"],
        "airline": event["airline"],
        "origin": event["origin"],
    }
    future = publisher.publish(topic_path, data, **attributes)
    return future.result()


def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Publishing to: {topic_path}")
    print("Press Ctrl+C to stop\n")
    count = 0
    try:
        while True:
            event = generate_flight_event()
            msg_id = publish_event(event, publisher, topic_path)
            count += 1
            icon = "OK" if event["status"] == "ON_TIME" else "!!"
            print(
                f"[{count:04d}] {icon} {event['flight_id']} "
                f"{event['origin']} -> {event['destination']} | "
                f"delay={event['delay_minutes']}m | "
                f"reason={event['delay_reason']} | "
                f"msg_id={msg_id}"
            )
            time.sleep(2)
    except KeyboardInterrupt:
        print(f"\nStopped. Published {count} events.")


if __name__ == "__main__":
    main()
