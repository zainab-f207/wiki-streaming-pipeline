import json
import time
import requests
from kafka import KafkaProducer

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {"User-Agent": "de-learning-project (github.com/zainab-f207)"}
TOPIC = "wiki_changes"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda row: json.dumps(row).encode("utf-8"),
)


def make_row(event):
    # Log events have no length, so these can be missing
    length = event.get("length") or {}
    old = length.get("old")
    new = length.get("new")
    if old is not None and new is not None:
        byte_change = new - old
    else:
        byte_change = None

    return {
        "event_id": event["meta"]["id"],
        "event_time": event["meta"]["dt"],
        "wiki": event.get("wiki"),
        "type": event.get("type"),
        "title": event.get("title"),
        "user": event.get("user"),
        "is_bot": event.get("bot"),
        "byte_change": byte_change,
    }


count = 0
reconnects = 0
start = time.time()
print("Sending events to Kafka. Press Ctrl+C to stop.")

try:
    while True:
        try:
            response = requests.get(URL, headers=HEADERS, stream=True, timeout=30)
            for line in response.iter_lines():
                line = line.decode("utf-8")
                if not line.startswith("data: "):
                    continue

                event = json.loads(line[6:])
                producer.send(TOPIC, make_row(event))

                count = count + 1
                if count % 1000 == 0:
                    print("Sent", count)
        except requests.exceptions.RequestException as error:
            reconnects = reconnects + 1
            print("Connection lost:", type(error).__name__)
            print("Reconnecting in 5 seconds... (reconnect number", reconnects, ")")
            time.sleep(5)
except KeyboardInterrupt:
    print("Stopping...")
finally:
    producer.flush()
    seconds = round(time.time() - start)
    print("Sent", count, "events in", seconds, "seconds,", reconnects, "reconnects")
