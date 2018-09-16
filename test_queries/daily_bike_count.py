import pandas as pd
import time
import sys
sys.path.append("..")
import util_functions as uf

if __name__ == "__main__":
    # Start timer
    start_time = time.time()
    # Connect to AWS
    uf.set_env_path()
    conn, cur = uf.local_connect()

    # Determine Number of Bikes Available by Operator from March 1, 2018 - August 31st, 2018
    df = pd.read_sql("""
                    /* Bike Counts Except Ofo */
                    (SELECT
                    created::date as date, 
                    provider, 
                    COUNT(DISTINCT bike_id) as bike_count
                    FROM 
                    (select * 
                    from bike_locations
                    WHERE created::date >= '2018-03-01'
                    AND created::date <= '2018-08-30'
                    limit 10) as bike_locations
                    WHERE provider != 'ofo' AND 
                        (provider != 'limebike' or 
                        raw->'attributes'->>'vehicle_type'='bike' or 
                        raw->'attributes'->>'vehicle_type' is null)                        
                    GROUP BY provider, created::date)
                    UNION
                    /* Bike Counts for  Ofo */
                    (SELECT 
                    created as date, 
                    'ofo' as provider, 
                    max("count") as bike_count
                    from 
                    (select 
                    count(bike_id), 
                    bike_id, 
                    created::date 
                    from (SELECT * 
                          FROM bike_locations 
                          WHERE provider='ofo' AND
                          created::date >= '2018-03-01'
                          AND created::date <= '2018-08-30'
                          limit 10) as ofo
                    group by bike_id, created::date) as counts_per_batch
                    group by created
                    )
                    order by date, provider
                     """, con=conn)
    #pivot dataframe
    pivot_df = df.pivot(index='date', columns='provider', values='bike_count')
    pivot_df.to_csv("daily_bike_count.csv")
    # print time spent
    print("--- {} seconds ---".format(time.time() - start_time))
