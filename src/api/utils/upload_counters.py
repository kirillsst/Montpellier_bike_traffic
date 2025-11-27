def upload_counter_to_supabase(row, supabase):
    supabase.table("counters").insert({
        "ecocounter_uid": row["id"], 
        "deviceType": row["deviceType"]["value"],
        "lane_id": row["laneId"]["value"],
        "lat": row["location"]["value"]["coordinates"][0],
        "lon": row["location"]["value"]["coordinates"][1],
        "reversed_lane": row["reversedLane"]["value"],
        "vehicle_type": row["vehicleType"]["value"]
    }).execute()
