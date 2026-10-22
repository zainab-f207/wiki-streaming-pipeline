import json
import requests

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {"User-Agent": "de-learning-project (github.com/zainab-f207)"}

response = requests.get(URL, headers=HEADERS, stream=True)

count = 0
for line in response.iter_lines():
    line = line.decode("utf-8")

    # Only lines that start with "data: " hold an event
    if not line.startswith("data: "):
        continue

    event = json.loads(line[6:])

    # Log events have no length, so these can be missing
    length = event.get("length") or {}
    old = length.get("old")
    new = length.get("new")
    if old is not None and new is not None:
        byte_change = new - old
    else:
        byte_change = None

    row = {
        "event_id": event["meta"]["id"],
        "event_time": event["meta"]["dt"],
        "wiki": event["wiki"],
        "type": event["type"],
        "title": event["title"],
        "user": event["user"],
        "is_bot": event["bot"],
        "byte_change": byte_change,
    }
    print(row)

    count = count + 1
    if count == 10:
        break
