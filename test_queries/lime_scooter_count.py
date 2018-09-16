import pandas as pd
import sys
import time
sys.path.append("..")
import util_functions as uf

if __name__ == "__main__":
    # Start timer
    start_time = time.time()
    # Connect to DB
    uf.set_env_path()
    conn, cur = uf.local_connect()

    # Determine Number of Scooters Available by for Lime, has to be reverse engineered
    df = pd.read_sql("""
                    SELECT
                    locations.date,
                    count(distinct locations.bike_id) as daily_scooter_count
                    from 
                    (select distinct
                    created::date as date, bike_id 
                    from bike_locations
                    where provider='limebike') as locations
                    JOIN
                    (SELECT DISTINCT bike_id
                    from bike_locations
                    where raw->'attributes'->>'vehicle_type'='scooter') as scooters
                    on locations.bike_id = scooters.bike_id
                    group by 1
                    order by 1;
                     """, con=conn)
    df.to_csv("daily_lime_scooter_count.csv")
    # print time spent
    print("--- {} seconds ---".format(time.time() - start_time))