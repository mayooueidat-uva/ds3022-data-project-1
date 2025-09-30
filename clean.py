import os
import duckdb
import logging

# configuring the logging mechanism
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

# defining function to clean our data
def clean_parquet():
    con = None
    try:
        # connect to local database
        con = duckdb.connect(database="TEST.db", read_only=False)
        logger.info("Connected to DuckDB instance")
        
        # removing trips that are more than one day in length for YELLOW cabs
        con.execute(f"""
            ALTER TABLE yellow_tripdata_06
            ADD COLUMN trip_duration DOUBLE;

           UPDATE yellow_tripdata_06
           SET trip_duration = EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime));

           DELETE FROM yellow_tripdata_06
           WHERE trip_duration > 86400;
        """)
        logger.info("Removed days that are more than 24 hr in length from yellow cab table")
        
        # removing trips that are more than one day in length for GREEN cabs
        con.execute(f"""    
            ALTER TABLE green_tripdata_06
            ADD COLUMN trip_duration DOUBLE;

           UPDATE green_tripdata_06 
           SET trip_duration = EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime));

           DELETE FROM green_tripdata_06
           WHERE trip_duration > 86400;
        """)
        logger.info("Removed days that are more than 24 hr in length from green cab table")

        # removing duplicate values for YELLOW CABS
        con.execute(f"""
            CREATE TABLE yellow_tripdata_clean AS 
            SELECT DISTINCT * FROM yellow_tripdata_06;

            DROP TABLE yellow_tripdata_06;
            ALTER TABLE yellow_tripdata_clean RENAME TO yellow_tripdata_06;
        """)
        logger.info("Removed duplicate values for yellow cab table")

        # removing duplicate values for GREEN CABS
        con.execute(f"""
            CREATE TABLE green_tripdata_clean AS 
            SELECT DISTINCT * FROM green_tripdata_06;

            DROP TABLE green_tripdata_06;
            ALTER TABLE green_tripdata_clean RENAME TO green_tripdata_06;
        """)
        logger.info("Removed duplicate values for green cab table")
        
        # removing long trips from YELLOW CABS 
        con.execute(f"""
            DELETE FROM yellow_tripdata_06
            WHERE trip_distance > 100;
        """)
        logger.info("Removed long trips from yellow cab table")
        
        # removing long trips from GREEN CABS
        con.execute(f"""
            DELETE FROM green_tripdata_06
            WHERE trip_distance > 100;
        """)
        logger.info("Removed long trips from green cab table")
        
        # removing zero passengers from YELLOW CABS 
        con.execute(f""" 
            DELETE FROM yellow_tripdata_06
            WHERE passenger_count < 1;
        """)
        logger.info("Removed trips with zero passengers from yellow cab table")
        
        # removing zero passengers from GREEN CABS
        con.execute(f""" 
            DELETE FROM green_tripdata_06
            WHERE passenger_count < 1;
        """)
        logger.info("Removed trips with zero passengers from green cab table")

        # removing zero miles in length from YELLOW CABS
        con.execute(f"""
            DELETE FROM yellow_tripdata_06
            WHERE trip_distance < 1;
        """)
        logger.info("Removed zero-mile trips from yellow cab table")
        
        # removing zero miles in length from GREEN CABS
        con.execute(f"""
            DELETE FROM green_tripdata_06
            WHERE trip_distance < 1;
        """)
        logger.info("Removed zero-mile trips from green cab table")

    # error handling
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_parquet()
