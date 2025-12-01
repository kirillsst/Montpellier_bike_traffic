# from fastapi import APIRouter
# from src.api.utils.supabase_client import supabase
# from src.api.utils.fetch_ecocounter import fetch_all_counters
# from src.api.utils.upload_counters import upload_counter_to_supabase
# import pandas as pd
# from tqdm import tqdm

# router = APIRouter()

# @router.post("/init/load")
# def load_counters():
#     df = fetch_all_counters()  

#     for _, row in df.iterrows():
#         try:
#             upload_counter_to_supabase(row, supabase)
#         except Exception as e:
#             print(e)

#     return {"status": "ok", "rows_inserted": len(df)}