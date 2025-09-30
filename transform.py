import duckdb 
import os
import logging 

# configuring the logging mechanism
logging.basicConfig(
    filename='transform.log',
    filemode="a",
    style="{", 
    datefmt="%Y-%m-%d-%H:%M:%x", 
    level="INFO"
)
logger = logging.getLogger(__name__)

# defining function to transform our data 
def transform_parquet():
    con = None
    try:
    # connecting to database 
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

    # calculating vehicle emissions per trip - YELLOW cabs 
        con.execute(f"""
            ALTER TABLE yellow_trip_data ADD COLUMN trip_co2_kgs DOUBLE;
        
            UPDATE yellow_trip_data
            SET trip_co2_kgs = trip_distance * (
        SELECT co2_grams_per_mile FROM vehicle_emissions WHERE vehicle_type = 'yellow_taxi') / 1000;
        """)
        logger.info("created column for co2 trip emissions for YELLOW cabs") 

    # calculating vehicle emissions per trip - GREEN cabs 
        con.execute(f"""
            ALTER TABLE green_trip_data ADD COLUMN trip_co2_kgs DOUBLE;
        
            UPDATE green_trip_data
            SET trip_co2_kgs = trip_distance * (
        SELECT co2_grams_per_mile FROM vehicle_emissions WHERE vehicle_type = 'green_taxi') / 1000;
        """)
        logger.info("created column for co2 trip emissions for GREEN cabs") 

    # calculating mph - YELLOW cabs 
        con.execute(f"""
            ALTER TABLE yellow_trip_data
            ADD COLUMN hours_diff DOUBLE;

            UPDATE yellow_trip_data
            SET hours_diff = EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0;

            ALTER TABLE yellow_trip_data
            ADD COLUMN avg_mph DOUBLE;

            UPDATE yellow_trip_data
            SET avg_mph = trip_distance / hours_diff;
        """)
        logger.info("made column for average MPH for each YELLOW cab trip")

    # calculating mph - GREEN cabs 
        con.execute(f"""
            ALTER TABLE green_trip_data
            ADD COLUMN hours_diff DOUBLE;

            UPDATE green_trip_data
            SET hours_diff = EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0;

            ALTER TABLE green_trip_data
            ADD COLUMN avg_mph DOUBLE;

            UPDATE green_trip_data
            SET avg_mph = trip_distance / hours_diff;
        """)
        logger.info("made column for average MPH for each GREEN cab trip")

        
    # creating column for hour of day of pickups - YELLOW cabs
        con.execute(f"""
            ALTER TABLE yellow_trip_data
            ADD COLUMN hour_of_day INTEGER;
        
            UPDATE yellow_trip_data
            SET hour_of_day = EXTRACT(HOUR FROM tpep_pickup_datetime);
        """)
        logger.info("created column for hour of day for YELLOW cab trips")

    # creating column for hour of day of pickups - GREEN cabs
        con.execute(f"""
            ALTER TABLE green_trip_data
            ADD COLUMN hour_of_day INTEGER;
        
            UPDATE green_trip_data
            SET hour_of_day = EXTRACT(HOUR FROM lpep_pickup_datetime);
        """)
        logger.info("created column for hour of day for GREEN cab trips")
        
    # creating column for day of week of pickups - YELLOW cabs
        con.execute(f"""
            ALTER TABLE yellow_trip_data
            ADD COLUMN day_of_week VARCHAR;
            
            UPDATE yellow_trip_data
            SET day_of_week = dayname(tpep_pickup_datetime);
        """)
        logger.info("created column for day of week for YELLOW cab trips")
        
    # creating column for day of week of pickups - GREEN cabs
        con.execute(f"""
            ALTER TABLE green_trip_data
            ADD COLUMN day_of_week VARCHAR;
            
            UPDATE green_trip_data
            SET day_of_week = dayname(lpep_pickup_datetime);
        """)
        logger.info("created column for day of week for GREEN cab trips")
        
    # creating column for week of year of pickups - YELLOW cabs
        con.execute(f"""
            ALTER TABLE yellow_trip_data
            ADD COLUMN week_of_year INTEGER;
            
            UPDATE yellow_trip_data
            SET week_of_year = EXTRACT(WEEK FROM tpep_pickup_datetime);
        """)
        logger.info("created column for week of year for YELLOW cab trips")
        
    # creating column for week of year of pickups - GREEN cabs
        con.execute(f"""
            ALTER TABLE green_trip_data
            ADD COLUMN week_of_year INTEGER;
            
            UPDATE green_trip_data
            SET week_of_year = EXTRACT(WEEK FROM lpep_pickup_datetime);
        """)
        logger.info("created column for week of year for GREEN cab trips")

    # creating column for month of year of pickups - YELLOW cabs
        con.execute(f"""
            ALTER TABLE yellow_trip_data
            ADD COLUMN month_of_year VARCHAR;
            
            UPDATE yellow_trip_data
            SET month_of_year = monthname(tpep_pickup_datetime);
        """)
        logger.info("created column for month of year for YELLOW cab trips")
        
    # creating column for month of year of pickups - GREEN cabs
        con.execute(f"""
            ALTER TABLE green_trip_data
            ADD COLUMN month_of_year VARCHAR;
            
            UPDATE green_trip_data
            SET month_of_year = monthname(lpep_pickup_datetime);
        """)
        logger.info("created column for month of year for GREEN cab trips")

    # error handling
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_parquet()
