import os
import duckdb
import logging

def clean_parquet():
    con = None # what does this mean. 
    try:
        # connect to local database
        con = duckdb.connect(database="TEST.db", read_only=False)
        # removing trips that are more than one day in length YELLOW
        con.execute(f"""
            ALTER TABLE yellow_trip_data4
            ADD COLUMN trip_duration DOUBLE;

           UPDATE yellow_trip_data4
           SET trip_duration = EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime));

           DELETE FROM yellow_trip_data4
           WHERE trip_duration > 86400;
        """)
        # removing trips that are more than one day in length GREEN
        con.execute(f"""    
            ALTER TABLE green_tripdata_01 
            ADD COLUMN trip_duration DOUBLE;

           UPDATE yellow_tripdata_01 
           SET trip_duration = EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime));

           DELETE FROM yellow_tripdata_01
           WHERE trip_duration > 86400;
        """)
        # removing duplicate values (done - just needs test run) 
        con.execute(f"""
            CREATE TABLE yellow_tripdata_clean AS 
            SELECT DISTINCT * FROM yellow_tripdata_01;

            DROP TABLE yellow_tripdata_01;
            ALTER TABLE yellow_tripdata_clean RENAME TO yellow_tripdata_01;
        """)
        # removing long trips (done - just needs test run) 
        too_old = con.execute(f"""
            DELETE FROM yellow_tripdata_01
            WHERE trip_distance > 100;
        """)
        # removing zero passengers (done - just needs test run) 
        con.execute(f""" 
            DELETE FROM yellow_tripdata_01
            WHERE passenger_count < 1;
        """)
        # removing zero miles in length (done - just needs test run) 
        con.execute(f"""
            DELETE FROM yellow_tripdata_01 
            WHERE trip_distance < 1;
        """)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_parquet()
