from fastapi import APIRouter
from src.api.utils.supabase_client import supabase

router = APIRouter()

@router.get("/debug/counters_range")
# 
def debug_counters_range():
    all_rows = []
    limit = 1000 
    offset = 0

    while True:
        resp = (
            supabase.table("counters")
            .select("timestamp")
            .order("timestamp")
            .range(offset, offset + limit - 1)
            .execute()
        )

        data = resp.data
        if not data:
            break

        all_rows.extend(data)
        offset += limit

    if not all_rows:
        return {"error": "Empty"}

    timestamps = [row["timestamp"] for row in all_rows]

    return {
        "min_timestamp": min(timestamps),
        "max_timestamp": max(timestamps),
        "total_rows": len(timestamps)
    }
