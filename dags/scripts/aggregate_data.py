import pandas as pd
import os

SILVER_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'silver', 'rides_clean.csv')
GOLD_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'gold', 'rides_agg.csv')

def aggregate_data():
    if not os.path.exists(SILVER_PATH):
        print("Silver data not found. Run transform_data first.")
        return

    df = pd.read_csv(SILVER_PATH)

    print("📊 Loaded Silver cleaned data.")
    print(f"Rows before aggregation: {len(df)}")

    # Example GOLD aggregation (you can change this later)
    df_gold = (
        df.groupby(['driver'])
        .agg(
            total_rides=('ride_id', 'count'),
            total_distance=('distance_km', 'sum'),
            total_earnings=('total_usd', 'sum'),
            avg_fare=('fare_usd', 'mean'),
            avg_tip=('tip_usd', 'mean')
        )
        .reset_index()
    )

    # Make sure gold folder exists
    gold_dir = os.path.dirname(GOLD_PATH)
    os.makedirs(gold_dir, exist_ok=True)

    df_gold.to_csv(GOLD_PATH, index=False)

    print(f"Gold aggregated data saved to: {GOLD_PATH}")
    print(f"Rows after aggregation: {len(df_gold)}")

if __name__ == "__main__":
    aggregate_data()
