import sys
sys.path.append("..")
import pandas as pd
import util_functions as uf
import altair as alt
import json
import requests

alt.renderers.enable('notebook')
alt.themes.enable('opaque')

def lime_totals(con):
    # Query Dockless Start by ANC and Overlaps
    return pd.read_sql("""SELECT DISTINCT
                            total_lime.date,
                            daily_lime_count as total,
                            case when daily_scooter_count is null then daily_lime_count
                            else daily_lime_count - daily_scooter_count end as bikes,					   
                            daily_scooter_count as scooters
                            FROM
                            /* Count of Total Lime Vehicles per Day*/
                            (
                            SELECT
                            created::date as date,
                            count(distinct bike_id) as daily_lime_count
                            from bike_locations
                            where provider='limebike'
                            GROUP BY 1) as total_lime
                            /* Count of Total Lime Scooters per Day*/
                            LEFT JOIN
                            (SELECT
                            locations.date,
                            count(distinct locations.bike_id) as daily_scooter_count
                            FROM (SELECT DISTINCT
                                  created::date as date, bike_id 
                                  from bike_locations
                                  where provider='limebike') as locations
                            JOIN
                            (SELECT DISTINCT bike_id
                                             from bike_locations
                                             where raw->'attributes'->>'vehicle_type'='scooter') as scooters
                                             on locations.bike_id = scooters.bike_id
                            GROUP BY 1
                            ORDER BY 1) as scoots_lime
                            on scoots_lime.date = total_lime.date
;
                """, con=con)

if __name__ == "__main__":
    uf.set_env_path()
    conn, cur = uf.local_connect()
    # Return Dataframe of Percent of Trips by ANC
    df = lime_totals(con=conn)
    df.set_index(['anc_id', 'end_geo_overlap'], inplace=True)
    unstacked_df = df.unstack(level=-1)
    unstacked_df = pd.DataFrame(unstacked_df.to_records())
    unstacked_df.columns = [hdr.replace("('", "").replace("', '", "_").replace("')", "") for hdr in unstacked_df.columns]
    # Merge dataframe to append percentages to Geo data
    gdf_merged = gdf.merge(unstacked_df, left_on='ANC_ID', right_on='anc_id', how='inner')
    # Determine Center of each Pologygon for labels
    gdf_merged['centroid_lon'] = gdf_merged['geometry'].centroid.x
    gdf_merged['centroid_lat'] = gdf_merged['geometry'].centroid.y
     # Create JSON for data layer
    json_features = json.loads(gdf_merged.to_json())
    data_geo = alt.Data(values=json_features['features'])
    # Create JSON for labels layer
    json_label = json.loads(gdf_merged[['ANC_ID', 'centroid_lon', 'centroid_lat']].to_json(orient='records'))
    label_geo = alt.Data(values=json_label)
    # Generate Chloropaths
    geo_yes_chart = gen_chloro(title='DoBi Trips Ending Within CaBi Service Area', color_column='properties.end_perc_1')
    geo_no_chart = gen_chloro(title='DoBi Trips Ending Outside CaBi Service Area', color_column='properties.end_perc_0')
    combined_chloro = (geo_yes_chart | geo_no_chart).configure_legend(orient='bottom-left', offset = 2)
    # combined_chloro = alt.Chart.configure_legend(combined_chloro, orient='bottom-left')
    combined_chloro.save('geo_end_chloro_ggw.html')

