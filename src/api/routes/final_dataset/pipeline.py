# final_dataset/pipeline.py
import pandas as pd
from src.api.utils.supabase_client import supabase
from datetime import datetime, timezone

FINAL_TABLE = "counters_final"

def run_final_pipeline():
    print("--- Démarrage de l'agrégation finale ---")

    # ---------------------------------------------------------
    # 1. CHARGEMENT DES DONNÉES DE SUPABASE
    # ---------------------------------------------------------
    print("1. Chargement des données depuis Supabase...")

    def fetch_all(table_name: str):
        """Récupère toutes les lignes d'une table Supabase en paginant par 1000."""
        all_rows = []
        offset = 0
        limit = 1000
        while True:
            resp = supabase.table(table_name).select("*").range(offset, offset + limit - 1).execute()
            rows = resp.data
            if not rows:
                break
            all_rows.extend(rows)
            offset += limit
        return pd.DataFrame(all_rows)

    # A. Compteurs vélos
    df_velo = fetch_all("counters_clean")
    df_velo['timestamp'] = pd.to_datetime(df_velo['timestamp'], utc=True)
    print(f"   - Vélos      : {len(df_velo)} lignes ({df_velo['name'].nunique()} compteurs)")

    # B. Météo historique
    df_meteo = fetch_all("meteo_history")
    df_meteo['timestamp'] = pd.to_datetime(df_meteo['time'], utc=True)
    df_meteo = df_meteo.drop(columns=['time'], errors='ignore')
    print(f"   - Météo      : {len(df_meteo)} lignes")

    # C. Calendrier
    df_cal = fetch_all("calendar")
    df_cal['date'] = pd.to_datetime(df_cal['date'])
    print(f"   - Calendrier : {len(df_cal)} jours")

    # ---------------------------------------------------------
    # 2. FULL HOURLY GRID
    # ---------------------------------------------------------
    print("\n2. Création du full hourly grid pour tous les compteurs...")

    compteur_names = df_velo['name'].unique()
    full_time_range = pd.date_range(df_velo['timestamp'].min(), df_velo['timestamp'].max(), freq='h', tz='UTC')

    df_grid = pd.MultiIndex.from_product([compteur_names, full_time_range], names=['name', 'timestamp']).to_frame(index=False)

    coords = df_velo.groupby('name')[['latitude', 'longitude']].first().reset_index()
    df_grid = df_grid.merge(coords, on='name', how='left')

    df_grid = df_grid.merge(df_velo[['name','timestamp','intensity']], on=['name','timestamp'], how='left')
    df_grid['intensity'] = df_grid['intensity'].fillna(0) 

    print(f"   - Total lignes grid : {len(df_grid)}")

    # ---------------------------------------------------------
    # 3. FUSION MÉTÉO
    # ---------------------------------------------------------
    print("\n3. Fusion Météo (Hourly join)...")
    df_merged = pd.merge(df_grid, df_meteo, on='timestamp', how='left')

    for col in ['temperature_2m','precipitation','windspeed_10m']:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].fillna(0)

    print(f"Après merge météo : {len(df_merged)}")

    # ---------------------------------------------------------
    # 4. FUSION CALENDRIER
    # ---------------------------------------------------------
    print("4. Fusion Calendrier (Daily join)...")
    df_merged['date_join'] = df_merged['timestamp'].dt.tz_convert(None).dt.normalize()
    df_final = pd.merge(df_merged, df_cal, left_on='date_join', right_on='date', how='left')
    df_final = df_final.drop(columns=['date_join','date'])

    for col in ['jour_semaine','is_weekend','nom_jour','is_ferie','is_vacances','is_jour_ouvre']:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna(0)

    print(f"Après merge calendrier : {len(df_final)}")

    # ---------------------------------------------------------
    # 5. INSERTION DANS SUPABASE
    # ---------------------------------------------------------
    print("\n5. Insertion dans Supabase (counters_final)...")

    columns_needed = [
        'name', 'timestamp', 'intensity', 'latitude', 'longitude',
        'temperature_2m', 'precipitation', 'windspeed_10m',
        'jour_semaine', 'is_weekend', 'nom_jour', 'is_ferie',
        'is_vacances', 'is_jour_ouvre', 'created_at'
    ]

    if 'created_at' not in df_final.columns:
        df_final['created_at'] = datetime.now(timezone.utc)

    df_final_to_insert = df_final[columns_needed].copy()

    for col in df_final_to_insert.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]"]).columns:
        df_final_to_insert[col] = df_final_to_insert[col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    records = df_final_to_insert.to_dict(orient="records")
    chunk_size = 500

    for i in range(0, len(records), chunk_size):
        chunk = records[i:i+chunk_size]
        try:
            supabase.table(FINAL_TABLE).insert(chunk).execute()
            print(f"   ✔ {len(chunk)} lignes insérées")
        except Exception as e:
            print(f"   ❌ Erreur lors de l'insertion : {e}")

    print(f"\n✅ Pipeline final terminé. Total lignes : {len(df_final)}")
    return {"rows_final": len(df_final)}
