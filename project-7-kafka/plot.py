import pandas as pd
import matplotlib.pyplot as plt
import os, json

months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
month_dict = {}

for partition in range(4):
    file_path = f"files/partition-{partition}.json"
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            data = json.load(file)
            for month in months:
                if month in data:
                    years = data[month].keys()
                    latest_year = max(years)
                    month_dict[month] = data[month][latest_year]["avg"]

#print(month_dict)
month_series = pd.Series(month_dict)

fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
plt.tight_layout()
plt.savefig("/files/month.svg")
