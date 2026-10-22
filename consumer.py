import json
from collections import Counter
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "wiki_changes",
    bootstrap_servers="localhost:9092",
    group_id="stage5-counter",
    auto_offset_reset="earliest",
    value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    consumer_timeout_ms=5000,
)

total = 0
bots = 0
by_type = Counter()
by_wiki = Counter()

# Stops by itself after 5 seconds with no new messages
for message in consumer:
    row = message.value
    total = total + 1
    by_type[row["type"]] += 1
    by_wiki[row["wiki"]] += 1
    if row["is_bot"]:
        bots = bots + 1

consumer.close()

print("Messages read:", total)
print("By type:", by_type.most_common())
print("Top 5 wikis:", by_wiki.most_common(5))
if total > 0:
    print("Bot share:", round(100 * bots / total, 1), "%")
