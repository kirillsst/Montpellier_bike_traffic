from supabase_client import supabase

def check_last_timestamps():
    try:
        resp = (
            supabase
            .table("counters")
            .select("timestamp")
            .order("timestamp", desc=True)
            .limit(10)
            .execute()
        )
        print("\n=== LAST TIMESTAMPS IN TABLE `counters` ===")
        for row in resp.data:
            print(row["timestamp"])
    except Exception as e:
        print("Error:", e)

# Вызови
check_last_timestamps()
