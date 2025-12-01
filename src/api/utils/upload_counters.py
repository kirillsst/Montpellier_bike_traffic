import pandas as pd
from tqdm import tqdm
from src.api.utils.fetch_ecocounter import fetch_counter_timeseries
from src.api.config import DATE_FIN_CIBLE
from src.api.utils.supabase_client import supabase

# def upload_counter_to_supabase(row, supabase):
#     supabase.table("counters").insert({
#         "counter_id": row["id"], 
#         "deviceType": row["deviceType"]["value"],
#         "lane_id": row["laneId"]["value"],
#         "lat": row["location"]["value"]["coordinates"][0],
#         "lon": row["location"]["value"]["coordinates"][1],
#         "reversed_lane": row["reversedLane"]["value"],
#         "vehicle_type": row["vehicleType"]["value"]
#     }).execute()

def download_and_merge_timeseries(df_merged, PERIODES):
    all_dfs = []

    PERIODES = [
        ("2023-01-01T00:00:00", "2023-12-31T23:59:59"),
        ("2024-01-01T00:00:00", "2024-12-31T23:59:59"),
        ("2025-01-01T00:00:00", DATE_FIN_CIBLE)
    ]

    for _, row in tqdm(df_merged.iterrows(), total=len(df_merged)):
        api_id = row["id"]
        for start_date, end_date in PERIODES:
            try:
                df_temp = fetch_counter_timeseries(api_id, start_date, end_date)
                if not df_temp.empty:
                    df_temp["name"] = row["nom_csv"]
                    df_temp["latitude"] = row["latitude"]
                    df_temp["longitude"] = row["longitude"]
                    all_dfs.append(df_temp)
            except:
                continue

    if all_dfs:
        df_concat = pd.concat(all_dfs, ignore_index=True)
        df_final = df_concat.groupby(['name', 'latitude', 'longitude', 'timestamp'], as_index=False)['intensity'].sum()
        df_final = df_final.sort_values(by=["name", "timestamp"])
        df_final = df_final[df_final["timestamp"] <= DATE_FIN_CIBLE]

        # convert timestamp in string ISO for json (kirillsst)
        df_final['timestamp'] = df_final['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        return df_final
    return pd.DataFrame()

def upload_to_supabase(df_final):
    if df_final.empty:
        print("Aucune donnée à uploader.")
        return 0

    records = df_final.to_dict(orient="records")
    for i in range(0, len(records), 500):  # batch insert
        batch = records[i:i+500]
        supabase.table("counters").insert(batch).execute()
    return len(records)
