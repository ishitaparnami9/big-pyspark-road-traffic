import pandas as pd
import numpy as np
import json
import random
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)

# Parameters
def num_roads():
    return 20
def num_days():
    return 30
def max_vehicles_per_day():
    return 1000

def generate_road_info():
    road_ids = [f"R{str(i).zfill(3)}" for i in range(1, num_roads()+1)]
    names = [f"Road_{i}" for i in range(1, num_roads()+1)]
    cities = ["CityA", "CityB", "CityC"]
    lengths = np.round(np.random.uniform(1, 10, num_roads()), 2)
    lanes_options = [2, 3, 4, 5]
    speed_limits = [30, 40, 50, 60, 70, 80]

    rows = []
    for i in range(num_roads()):
        row = {
            "road_id": road_ids[i],
            "name": names[i],
            "city": random.choice(cities),
            "length_km": lengths[i],
            "lanes": random.choice(lanes_options),
            "speed_limit": random.choice(speed_limits)
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    df.to_csv("road_info.csv", index=False)
    return df

def generate_vehicle_count(road_info_df):
    dates = [datetime.today() - timedelta(days=x) for x in range(num_days())]
    data = []
    for road_id in road_info_df["road_id"]:
        for date in dates:
            data.append({
                "road_id": road_id,
                "date": date.strftime("%Y-%m-%d"),
                "cars": np.random.randint(50, max_vehicles_per_day()),
                "bikes": np.random.randint(20, max_vehicles_per_day()),
                "trucks": np.random.randint(10, max_vehicles_per_day()//2)
            })
    with open("vehicle_count.json", "w") as f:
        json.dump(data, f)
    return data

def generate_accidents(road_info_df):
    severity_levels = ["low", "medium", "high"]
    causes = ["speeding", "drunk driving", "mechanical failure", "weather", "distraction"]
    num_accidents = 15
    accidents = []
    dates = [datetime.today() - timedelta(days=np.random.randint(0, num_days())) for _ in range(num_accidents)]

    for _ in range(num_accidents):
        accident = {
            "date": dates[_].strftime("%Y-%m-%d"),
            "road_id": random.choice(road_info_df["road_id"]),
            "severity": random.choice(severity_levels),
            "cause": random.choice(causes),
            "fatalities": np.random.randint(0, 3)
        }
        accidents.append(accident)
    with open("accidents.txt", "w") as f:
        for acc in accidents:
            f.write(f"{acc['date']}, {acc['road_id']}, {acc['severity']}, {acc['cause']}, {acc['fatalities']}\n")
    return accidents

def generate_traffic_volume(road_info_df):
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    rows = []
    for road_id in road_info_df["road_id"]:
        for day in days_of_week:
            rows.append({
                "road_id": road_id,
                "avg_speed": np.round(np.random.uniform(20, 80), 2),
                "avg_volume": np.random.randint(100, 1000),
                "day_of_week": day
            })
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "traffic_volume.parquet")
    return df

def generate_live_stream(road_info_df):
    vehicle_ids = [f"V{str(i).zfill(5)}" for i in range(1, 1001)]
    live_data = []
    for _ in range(200):
        vehicle_id = random.choice(vehicle_ids)
        road_id = random.choice(road_info_df["road_id"])
        current_speed = np.round(np.random.uniform(0, 100), 2)
        timestamp = datetime.now() - timedelta(minutes=np.random.randint(0, 60))
        live_data.append({
            "vehicle_id": vehicle_id,
            "road_id": road_id,
            "current_speed": current_speed,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S")
        })
    return live_data

road_info_df = generate_road_info()
vehicle_count_data = generate_vehicle_count(road_info_df)
accidents_data = generate_accidents(road_info_df)
traffic_volume_df = generate_traffic_volume(road_info_df)
live_stream_data = generate_live_stream(road_info_df)

# Saving live stream data to a JSON for demonstration
with open("live_stream.json", "w") as f:
    json.dump(live_stream_data, f)

"Data generation completed."