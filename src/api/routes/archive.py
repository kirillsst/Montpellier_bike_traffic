from fastapi import APIRouter
from src.api.utils.io_utils import load_and_pivot_local_csv
from src.api.utils.fetch_ecocounter import fetch_api_counters_list
from src.api.utils.upload_counters import download_and_merge_timeseries, upload_to_supabase
from src.api.config import PATH_GEO_CSV

router = APIRouter()

@router.post("/archive")
def update_data():
    df_geo = load_and_pivot_local_csv(PATH_GEO_CSV)
    df_api = fetch_api_counters_list()
    
    if df_geo.empty or df_api.empty:
        return {"status": "error", "message": "Données manquantes"}

    df_merged = df_geo.merge(df_api, on="serial_number", how="inner")

    periods = [
        ("2023-01-01T00:00:00", "2023-12-31T23:59:59"),
        ("2024-01-01T00:00:00", "2024-12-31T23:59:59"),
        ("2025-01-01T00:00:00", "2025-11-29T23:59:59")
    ]

    df_final = download_and_merge_timeseries(df_merged, periods)
    rows_uploaded = upload_to_supabase(df_final)

    return {"status": "success", "rows_uploaded": rows_uploaded}
