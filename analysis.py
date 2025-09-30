import duckdb
import logging

# yellow
  print(con.execute(f"""
      SELECT *
      FROM yellow_tripdata_06
      WHERE trip_co2_kgs = (SELECT MAX(trip_co2_kgs) FROM yellow_tripdata_01);
  """).fetchdf())

# green
  print(con.execute(f"""
      SELECT *
      FROM green_tripdata_06
      WHERE trip_co2_kgs = (SELECT MAX(trip_co2_kgs) FROM green_tripdata_01);
  """).fetchdf())
