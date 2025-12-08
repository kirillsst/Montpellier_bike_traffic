# train_model_xgboost/loader.py

import pandas as pd

from src.api.utils.supabase_client import supabase  

# --- CONSTANTE GLOBALE DES FEATURES ---
FEATURES_XGBOOST = [
    'hour', 'dayofweek', 'month', 'year', 'dayofyear',
    'temperature_2m', 'precipitation', 'precipitation_class',
    'windspeed_10m', 'is_raining',
    'is_vacances', 'is_ferie', 'is_weekend'
]

TABLE_NAME = "counters_final"


def load_full_dataset():
    """
    Charge tout le dataset depuis Supabase avec pagination.
    """
    print(f"🌍 Connexion à Supabase (Table: {TABLE_NAME})...")

    all_rows = []
    offset = 0
    limit = 1000

    print("   ⏳ Téléchargement avec pagination...")

    while True:
        response = (
            supabase
            .table(TABLE_NAME)
            .select("*")
            .range(offset, offset + limit - 1)
            .execute()
        )

        rows = response.data
        if not rows:
            break

        all_rows.extend(rows)
        offset += limit

        if offset % 10000 == 0:
            print(f"      -> {offset} lignes récupérées...")

    df = pd.DataFrame(all_rows)
    print(f"   ✅ Chargé au total : {len(df)} lignes.")

    # timestamp => datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)

    return df


def create_features(df):
    """Ajoute les features temporelles nécessaires."""
    df = df.copy()

    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)

    df['hour'] = df['timestamp'].dt.hour
    df['dayofweek'] = df['timestamp'].dt.dayofweek
    df['month'] = df['timestamp'].dt.month
    df['year'] = df['timestamp'].dt.year
    df['dayofyear'] = df['timestamp'].dt.dayofyear

    return df
