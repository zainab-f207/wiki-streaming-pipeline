import json
from collections import Counter
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "wiki_changes",
    bootstrap_servers="localhost:9092",
    group_id="stage7-windows",
    auto_offset_reset="earliest",
    value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    consumer_timeout_ms=5000,
)

counts = {}  # one entry per minute

for message in consumer:
    row = message.value

    # "2026-10-21T13:40:08.984Z" -> "2026-10-21T13:40"
    minute = row["event_time"][:16]

    if minute not in counts:
        counts[minute] = Counter()
    counts[minute][row["type"]] += 1

consumer.close()

print(f"{'minute (UTC)':<17}{'edit':>7}{'new':>7}{'log':>7}{'categorize':>12}{'total':>8}")
for minute in sorted(counts):
    c = counts[minute]
    total = sum(c.values())
    print(f"{minute:<17}{c['edit']:>7}{c['new']:>7}{c['log']:>7}{c['categorize']:>12}{total:>8}")
