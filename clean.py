import os
import duckdb
import logging

def clean_parquet():
    con = None
    try:
        # connect to local database
        con = duckdb.connect(database="TEST.db", read_only=False)
        # removing trips that are more than one day in length YELLOW CABS
        con.execute(f"""
            ALTER TABLE yellow_tripdata_06
            ADD COLUMN trip_duration DOUBLE;

           UPDATE yellow_tripdata_06
           SET trip_duration = EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime));

           DELETE FROM yellow_tripdata_06
           WHERE trip_duration > 86400;
        """)
        # removing trips that are more than one day in length GREEN CABS
        con.execute(f"""    
            ALTER TABLE green_tripdata_06
            ADD COLUMN trip_duration DOUBLE;

           UPDATE green_tripdata_06 
           SET trip_duration = EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime));

           DELETE FROM green_tripdata_06
           WHERE trip_duration > 86400;
        """)
        # removing duplicate values YELLOW CABS
        con.execute(f"""
            CREATE TABLE yellow_tripdata_clean AS 
            SELECT DISTINCT * FROM yellow_tripdata_06;

            DROP TABLE yellow_tripdata_06;
            ALTER TABLE yellow_tripdata_clean RENAME TO yellow_tripdata_06;
        """)
        # removing duplicate values GREEN CABS
        con.execute(f"""
            CREATE TABLE green_tripdata_clean AS 
            SELECT DISTINCT * FROM green_tripdata_06;

            DROP TABLE green_tripdata_06;
            ALTER TABLE green_tripdata_clean RENAME TO green_tripdata_06;
        """)
        # removing long trips YELLOW CABS
        con.execute(f"""
            DELETE FROM yellow_tripdata_06
            WHERE trip_distance > 100;
        """)
        # removing long trips GREEN CABS
        con.execute(f"""
            DELETE FROM green_tripdata_06
            WHERE trip_distance > 100;
        """)
        # removing zero passengers YELLOW CABS
        con.execute(f""" 
            DELETE FROM yellow_tripdata_06
            WHERE passenger_count < 1;
        """)
        # removing zero passengers GREEN CABS
        con.execute(f""" 
            DELETE FROM green_tripdata_06
            WHERE passenger_count < 1;
        """)
        # removing zero miles in length YELLOW CABS
        con.execute(f"""
            DELETE FROM yellow_tripdata_06
            WHERE trip_distance < 1;
        """)
        # removing zero miles in length GREEN CABS
        con.execute(f"""
            DELETE FROM green_tripdata_06
            WHERE trip_distance < 1;
        """)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_parquet()
