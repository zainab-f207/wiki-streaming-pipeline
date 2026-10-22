import os
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("minute_counts.csv", parse_dates=["minute_utc"])

# One column per type, one row per minute
table = df.pivot(index="minute_utc", columns="type", values="events")

# Add the missing minutes as empty, so gaps show up as breaks in the line
table = table.asfreq("min")

ax = table.plot(figsize=(11, 5))
ax.set_title("Wikipedia recent changes per minute (UTC)")
ax.set_xlabel("Minute (UTC)")
ax.set_ylabel("Events")
ax.legend(title="type", loc="upper left", bbox_to_anchor=(1.01, 1))

os.makedirs("docs/images", exist_ok=True)
plt.savefig("docs/images/events-per-minute.png", dpi=120, bbox_inches="tight")
print("Saved docs/images/events-per-minute.png")
