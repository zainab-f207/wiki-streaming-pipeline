import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "wiki_changes",
    bootstrap_servers="localhost:9092",
    group_id="stage6-quality",
    auto_offset_reset="earliest",
    value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    consumer_timeout_ms=5000,
)

total = 0
duplicates = 0
seen_ids = set()
missing = {"wiki": 0, "type": 0, "title": 0, "user": 0}
edits_without_size = 0
first_time = None
last_time = None

for message in consumer:
    row = message.value
    total = total + 1

    # Have we seen this event_id before?
    if row["event_id"] in seen_ids:
        duplicates = duplicates + 1
    seen_ids.add(row["event_id"])

    # Count empty values
    for column in missing:
        if row[column] is None:
            missing[column] = missing[column] + 1

    if row["type"] == "edit" and row["byte_change"] is None:
        edits_without_size = edits_without_size + 1

    # Earliest and latest event time
    if first_time is None or row["event_time"] < first_time:
        first_time = row["event_time"]
    if last_time is None or row["event_time"] > last_time:
        last_time = row["event_time"]

consumer.close()

print("Messages read:", total)
print("Unique event_ids:", len(seen_ids))
print("Duplicates:", duplicates)
print("Missing values:", missing)
print("Edits without size:", edits_without_size)
print("First event:", first_time)
print("Last event:", last_time)
