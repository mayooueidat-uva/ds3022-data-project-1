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

# yellow hour of day 
  print(con.execute(f"""
    SELECT hour_of_day, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM yellow_tripdata_07
    GROUP BY hour_of_day
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# green hour of day 
  print(con.execute(f"""
    SELECT hour_of_day, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY hour_of_day
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# yellow day of week 
  print(con.execute(f"""
    SELECT day_of_week, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY day_of_week
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# green day of week 
  print(con.execute(f"""
    SELECT day_of_week, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY day_of_week
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# yellow week of year 
  print(con.execute(f"""
    SELECT week_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY week_of_year
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# green week of year
  print(con.execute(f"""
    SELECT week_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY week_of_year
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# yellow month of year
  print(con.execute(f"""
    SELECT month_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY month_of_year
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# green month of year 
  print(con.execute(f"""
    SELECT month_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY month_of_year
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())
