import pandas as pd
import os

# define paths
RAW_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'rides_raw.csv')
PROCESSED_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed', 'rides_clean.csv')

def transform_data():
    # check if raw data exists
    if not os.path.exists(RAW_PATH):
        print("❌ Raw data not found. Run extract_data.py first.")
        return

    # read raw data
    df = pd.read_csv(RAW_PATH)

    print("📊 Raw data loaded successfully.")
    print(f"Rows before cleaning: {len(df)}")

    # remove duplicates
    df.drop_duplicates(inplace=True)

    # fill missing city or driver with 'Unknown'
    df['city'] = df['city'].fillna('Unknown')
    df['driver'] = df['driver'].fillna('Unknown')

    # add new calculated columns
    df['total_usd'] = df['fare_usd'] + df['tip_usd']
    df['fare_per_km'] = (df['fare_usd'] / df['distance_km']).round(2)

    # convert ride_time to datetime
    df['ride_time'] = pd.to_datetime(df['ride_time'], errors='coerce')

    # drop rows with invalid dates or distances
    df = df.dropna(subset=['ride_time', 'distance_km'])

    # sort by ride time
    df.sort_values(by='ride_time', inplace=True)

    # save clean data
    output_dir = os.path.dirname(PROCESSED_PATH)
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(PROCESSED_PATH, index=False)

    print(f"✅ Cleaned data saved to: {PROCESSED_PATH}")
    print(f"Rows after cleaning: {len(df)}")

if __name__ == "__main__":
    transform_data()
