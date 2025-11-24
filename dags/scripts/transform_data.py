import pandas as pd
import os

BRONZE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'bronze', 'rides_raw.csv')
SILVER_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'silver', 'rides_clean.csv')

def transform_data():
    if not os.path.exists(BRONZE_PATH):
        print("❌ Bronze (raw) data not found. Run extract step first.")
        return

    df = pd.read_csv(BRONZE_PATH)

    print("📊 Loaded Bronze raw data")
    print(f"Rows before cleaning: {len(df)}")

    df.drop_duplicates(inplace=True)

    df['city'] = df['city'].fillna('Unknown')
    df['driver'] = df['driver'].fillna('Unknown')

    df['total_usd'] = df['fare_usd'] + df['tip_usd']
    df['fare_per_km'] = (df['fare_usd'] / df['distance_km']).round(2)

    df['ride_time'] = pd.to_datetime(df['ride_time'], errors='coerce')
    df = df.dropna(subset=['ride_time', 'distance_km'])

    df.sort_values(by='ride_time', inplace=True)

    silver_dir = os.path.dirname(SILVER_PATH)
    os.makedirs(silver_dir, exist_ok=True)
    df.to_csv(SILVER_PATH, index=False)

    print(f"⬜ Cleaned Silver data saved to: {SILVER_PATH}")
    print(f"Rows after cleaning: {len(df)}")

if __name__ == "__main__":
    transform_data()
