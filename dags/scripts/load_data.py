import psycopg2
import pandas as pd
import os

# file path for cleaned data
CLEAN_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed', 'rides_clean.csv')

def load_to_postgres():
    # read the cleaned CSV
    df = pd.read_csv(CLEAN_DATA_PATH)
    print(f"📊 Loaded {len(df)} rows from cleaned CSV")

    # connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",  # change this if your password is different
        port=5432
    )

    cur = conn.cursor()

    # create a table (if it doesn't exist already)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS rides (
        ride_id INT,
        city VARCHAR(50),
        driver VARCHAR(50),
        distance_km FLOAT,
        fare_usd FLOAT,
        tip_usd FLOAT,
        ride_time TIMESTAMP,
        total_usd FLOAT,
        fare_per_km FLOAT
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # insert data row by row
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO rides (ride_id, city, driver, distance_km, fare_usd, tip_usd, ride_time, total_usd, fare_per_km)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            int(row['ride_id']),
            row['city'],
            row['driver'],
            float(row['distance_km']),
            float(row['fare_usd']),
            float(row['tip_usd']),
            row['ride_time'],
            float(row['total_usd']),
            float(row['fare_per_km'])
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ All data successfully loaded into PostgreSQL!")

if __name__ == "__main__":
    load_to_postgres()
