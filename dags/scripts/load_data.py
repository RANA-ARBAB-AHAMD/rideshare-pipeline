import psycopg2
import pandas as pd
import os

# GOLD data path
GOLD_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'gold', 'rides_agg.csv')

def load_to_postgres():
    # read the GOLD CSV
    if not os.path.exists(GOLD_DATA_PATH):
        print("❌ Gold data not found. Run the Gold aggregation step first.")
        return

    df = pd.read_csv(GOLD_DATA_PATH)
    print(f"📊 Loaded {len(df)} aggregated rows from Gold CSV")

    # connect to PostgreSQL running inside Docker
    conn = psycopg2.connect(
        host="postgres",        # docker-compose service name
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )

    cur = conn.cursor()

    # create table (for Gold layer)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS rides_analytics (
        driver VARCHAR(50),
        total_rides INT,
        total_distance FLOAT,
        total_earnings FLOAT,
        avg_fare FLOAT,
        avg_tip FLOAT
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # insert data into table
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO rides_analytics (driver, total_rides, total_distance, total_earnings, avg_fare, avg_tip)
            VALUES (%s,%s,%s,%s,%s,%s);
        """, (
            row['driver'],
            int(row['total_rides']),
            float(row['total_distance']),
            float(row['total_earnings']),
            float(row['avg_fare']),
            float(row['avg_tip'])
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Gold analytics data successfully loaded into PostgreSQL!")

if __name__ == "__main__":
    load_to_postgres()
