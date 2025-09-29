import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    load_parquet()

months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
years = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute("""CREATE TABLE yellow_trip_data4(
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INTEGER,
        trip_distance DOUBLE
        );
        """)
        con.execute("""CREATE TABLE green_trip_data4(
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INTEGER,
        trip_distance DOUBLE
        );
        """)

        for year in years:
            for month in months:

                con.execute(f"""
                    INSERT INTO yellow_trip_data
                    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance 
                    FROM read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet");
                """)

                con.execute(f"""
                    INSERT INTO green_trip_data
                    SELECT lpep_pickup_datetime, lpep_dropoff_datetime, passenger_count, trip_distance 
                    FROM read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet");
                """)
        
        logger.info("Dropped table if exists")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()
