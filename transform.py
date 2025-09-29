import duckdb
import logging
import os
def transform_parquet():
    con = None
    try:
        con = duckdb.connect(database='synthdata.duckdb', read_only=False)
    # mph
        con.execute(f"""
            ALTER TABLE yellow_tripdata_01
            ADD COLUMN hours_diff DOUBLE;

            UPDATE yellow_tripdata_01
            SET hours_diff = EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0;

            ALTER TABLE yellow_tripdata_01
            ADD COLUMN avg_mph DOUBLE;

            UPDATE yellow_tripdata_01
            SET avg_mph = trip_distance / hours_diff;
            """)
    # hour of the day
        con.execute(f"""
            ALTER TABLE yellow_tripdata_01
            ADD COLUMN hour_of_day INTEGER;
        
            UPDATE yellow_tripdata_01
            SET hour_of_day = EXTRACT(HOUR FROM tpep_pickup_datetime));
            """)
    # day of the week 
        con.execute(f"""
            ALTER TABLE yellow_tripdata_01
            ADD COLUMN day_of_week VARCHAR;
            
            UPDATE yellow_tripdata_01
            SET day_of_week = dayname(tpep_pickup_datetime);
            """)
    # week of the year
        con.execute(f"""
            ALTER TABLE yellow_tripdata_01
            ADD COLUMN week_of_year INTEGER;
            
            UPDATE yellow_tripdata_01
            SET week_of_year = EXTRACT(WEEK FROM tpep_pickup_datetime);
            """)
    # month of the year
        con.execute(f"""
            ALTER TABLE yellow_tripdata_01
            ADD COLUMN month_of_year VARCHAR;
            
            UPDATE yellow_tripdata_01
            SET month_of_year = monthname(tpep_pickup_datetime);
            """)
    # vehicle emissions (come back to this) 
        con.execute(f"""
            SELECT y.trip_distance, co2.co2_grams_per_mile
            FROM yellow_trip_data y;""")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_parquet()
