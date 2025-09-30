import duckdb
import os
import logging

# defining list of months/years to iterate through 
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
years = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]

# configuring the logging mechanism
logging.basicConfig(
    filename='load.log',
    filemode="a",
    style="{", 
    datefmt="%Y-%m-%d-%H:%M:%x", 
    level="INFO"
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    load_parquet()

# defining function to load files 
def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # create vehicle emissions table, already populated
        con.execute("""CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv("https://raw.githubusercontent.com/mayooueidat-uva/ds3022-data-project-1/refs/heads/main/data/vehicle_emissions.csv");
        """)
        logger.info("created pre-populated vehicle emissions table")

        # create empty YELLOW CAB table
        con.execute("""CREATE TABLE yellow_trip_data4(
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE
            );
        """)
        logger.info("created yellow cab table")

        # create empty GREEN CAB table 
        con.execute("""CREATE TABLE green_trip_data4(
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE
            );
        """)
        logger.info("created green cab table")

        # pulling parquet files to populate cab tables 
        for year in years:
            for month in months:
                # populating YELLOW CAB table
                con.execute(f"""
                    INSERT INTO yellow_trip_data
                    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance 
                    FROM read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet");
                """)
                logger.info(f"added yellow cab data from year {year} month {month}") 
                # populating GREEN CAB table
                con.execute(f"""
                    INSERT INTO green_trip_data
                    SELECT lpep_pickup_datetime, lpep_dropoff_datetime, passenger_count, trip_distance 
                    FROM read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet");
                """)
                logger.info(f"added green cab data from year {year} month {month}") 

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()
