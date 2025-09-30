import duckdb
import logging

# single largest carbon producing trip of the date range - YELLOW
  print("carbon heaviest trip for yellow cabs\n",con.execute(f"""
      SELECT *
      FROM yellow_tripdata_06
      WHERE trip_co2_kgs = (SELECT MAX(trip_co2_kgs) FROM yellow_tripdata_01);
    """).fetchdf())

# single largest carbon producing trip of the date range - GREEN
  print("carbon heaviest trip for green cabs\n",con.execute(f"""
      SELECT *
      FROM green_tripdata_06
      WHERE trip_co2_kgs = (SELECT MAX(trip_co2_kgs) FROM green_tripdata_01);
    """).fetchdf())

# carbon heaviest hour of day - YELLOW 
  print("carbon heaviest hour for yellow cabs\n",con.execute(f"""
    SELECT hour_of_day, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM yellow_tripdata_07
    GROUP BY hour_of_day
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# carbon lightest hour of day - YELLOW 
  print("carbon lightest hour for yellow cabs\n",con.execute(f"""
    SELECT hour_of_day, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM yellow_tripdata_07
    GROUP BY hour_of_day
    ORDER BY avg_co2 ASC
    LIMIT 1;
  """).fetchdf())

# carbib heaviest hour of day - GREEN
  print("carbon heaviest hour for green cabs\n",con.execute(f"""
    SELECT hour_of_day, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY hour_of_day
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# carbon lightest hour of day - GREEN
  print("carbon lightest hour for green cabs\n",con.execute(f"""
    SELECT hour_of_day, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY hour_of_day
    ORDER BY avg_co2 ASC
    LIMIT 1;
  """).fetchdf())

# carbon heaviest day of week - YELLOW
  print("carbon heaviest day of week for yellow cabs\n",con.execute(f"""
    SELECT day_of_week, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY day_of_week
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# carbon lightest day of week - YELLOW
  print("carbon lightest day of week for yellow cabs\n",con.execute(f"""
    SELECT day_of_week, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY day_of_week
    ORDER BY avg_co2 ASC
    LIMIT 1;
  """).fetchdf())

# carbon heaviest day of week - GREEN
  print("carbon heaviest day of week for green cabs\n",con.execute(f"""
    SELECT day_of_week, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY day_of_week
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# carbon lightest day of week - GREEN
  print("carbon lightest day of week for green cabs\n",con.execute(f"""
    SELECT day_of_week, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY day_of_week
    ORDER BY avg_co2 ASC
    LIMIT 1;
  """).fetchdf())

# carbon heaviest week of year - YELLOW  
  print("carbon heaviest week of year for yellow cabs\n",con.execute(f"""
    SELECT week_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY week_of_year
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# carbon lightest week of year - YELLOW  
  print("carbon lightest week of year for yellow cabs\n",con.execute(f"""
    SELECT week_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY week_of_year
    ORDER BY avg_co2 ASC
    LIMIT 1;
  """).fetchdf())

# carbon heaviest week of year - GREEN  
  print("carbon heaviest week of year for green cabs\n",con.execute(f"""
    SELECT week_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY week_of_year
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# carbon lightest week of year - GREEN  
  print("carbon lightest week of year for green cabs\n",con.execute(f"""
    SELECT week_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY week_of_year
    ORDER BY avg_co2 ASC
    LIMIT 1;
  """).fetchdf())

# carbon heaviest month of year - YELLOW
  print("carbon lightest week of year for green cabs\n",con.execute(f"""
    SELECT month_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM yellow_tripdata_07
    GROUP BY month_of_year
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# carbon lightest month of year - YELLOW
  print("carbon lightest week of year for green cabs\n",con.execute(f"""
    SELECT month_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM yellow_tripdata_07
    GROUP BY month_of_year
    ORDER BY avg_co2 ASC
    LIMIT 1;
  """).fetchdf())

# carbon heaviest month of year - GREEN
  print("carbon heaviest month of year for green cabs\n",con.execute(f"""
    SELECT month_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY month_of_year
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())

# carbon lightest month of year - GREEN
  print("carbon lightest month of year for green cabs\n",con.execute(f"""
    SELECT month_of_year, 
    AVG(trip_co2_kgs) AS avg_co2
    FROM green_tripdata_07
    GROUP BY month_of_year
    ORDER BY avg_co2 DESC
    LIMIT 1;
  """).fetchdf())
