from src.api.utils.supabase_client import supabase
import pandas as pd

# ------------------------------
# Chargement de tous les compteurs à partir du tableau counters
# ------------------------------
def load_counters_from_db():
    response = supabase.table("counters").select("*").execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

# ------------------------------
# Chargement du jeu de données nettoyé dans counters_clean
# ------------------------------
def upload_counters_clean(df_final, batch_size=500):
    # pour une intégration future
    if 'day_of_week' not in df_final.columns:
        df_final['day_of_week'] = df_final['timestamp'].dt.dayofweek
    if 'meteo_data' not in df_final.columns:
        df_final['meteo_data'] = None
    if 'calendar_data' not in df_final.columns:
        df_final['calendar_data'] = None

    records = df_final.to_dict(orient="records")
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        supabase.table("counters_clean").upsert(batch).execute()
    print(f"✅ Uploaded {len(records)} strings in counters_clean")
