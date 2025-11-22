import pandas as pd
import random
import os
from datetime import datetime, timedelta

def generate_fake_data(num_rows=100):
    # define some sample data
    cities = ['Budapest', 'Karachi', 'London', 'New York']
    drivers = ['Ali', 'John', 'Sarah', 'David', 'Emma', 'Khan']

    data = []
    for _ in range(num_rows):
        ride_time = datetime.now() - timedelta(days=random.randint(0, 10))
        distance_km = round(random.uniform(1, 30), 2)
        fare = round(distance_km * random.uniform(1.5, 3.5), 2)
        tip = round(fare * random.uniform(0.05, 0.2), 2)
        data.append({
            'ride_id': random.randint(1000, 9999),
            'city': random.choice(cities),
            'driver': random.choice(drivers),
            'distance_km': distance_km,
            'fare_usd': fare,
            'tip_usd': tip,
            'ride_time': ride_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    # make sure output folder exists
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')#this line make sure you go outside the current folder and than find data floder and inside that data folder you find raw.
    os.makedirs(output_dir, exist_ok=True) # it creates the folder path 

    output_path = os.path.join(output_dir, 'rides_raw.csv')
    pd.DataFrame(data).to_csv(output_path, index=False)

    print(f"✅ Raw data generated at: {output_path}")

if __name__ == "__main__":
    generate_fake_data()
