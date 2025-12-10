import pandas as pd
from src.api.utils.supabase_client import supabase  # <-- подключаем готовый клиент

# --- CONSTANTE GLOBALE DES FEATURES ---
FEATURES_XGBOOST = [
    'hour', 'dayofweek', 'month', 'year', 'dayofyear', 
    'temperature_2m', 'precipitation', 'precipitation_class', 'windspeed_10m', 'is_raining', 
    'is_vacances', 'is_ferie', 'is_weekend'
]

TABLE_NAME = "counters_final"
CUTOFF_DATE = pd.Timestamp("2025-11-30")  # можно оставить в коде, если не используем config


def load_full_dataset():
    """
    Charge tout le dataset depuis Supabase avec PAGINATION.
    Récupère par blocs de 1000 lignes jusqu'à épuisement.
    """
    print(f"🌍 Connexion à Supabase (Table: {TABLE_NAME})...")
    
    all_rows = []
    offset = 0
    limit = 1000
    
    print("   ⏳ Téléchargement en cours (pagination)...")
    
    while True:
        # On récupère un bloc de lignes (range est inclusif)
        response = supabase.table(TABLE_NAME).select("*").range(offset, offset + limit - 1).execute()
        rows = response.data
        
        if not rows:
            break
            
        all_rows.extend(rows)
        offset += limit
        
        if offset % 10000 == 0:
            print(f"      -> {offset} lignes récupérées...")

    # Conversion en DataFrame
    df = pd.DataFrame(all_rows)
    print(f"   ✅ Chargé au total : {len(df)} lignes.")

    # Conversion timestamp en datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
    
    return df


def create_features(df):
    """Crée les features temporelles pour XGBoost."""
    df = df.copy()
    
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)

    df['hour'] = df['timestamp'].dt.hour
    df['dayofweek'] = df['timestamp'].dt.dayofweek
    df['quarter'] = df['timestamp'].dt.quarter
    df['month'] = df['timestamp'].dt.month
    df['year'] = df['timestamp'].dt.year
    df['dayofyear'] = df['timestamp'].dt.dayofyear
    return df


def get_data_for_counter(df, counter_name):
    """Prépare X (Features) et y (Cible) pour un compteur."""
    # 1. Filtre compteur
    df_c = df[df['name'] == counter_name].copy()
    
    # 2. Création des features temporelles
    df_c = create_features(df_c)
    
    # 3. Split Temporel
    train = df_c[df_c['timestamp'] < CUTOFF_DATE]
    test = df_c[df_c['timestamp'] >= CUTOFF_DATE]
    
    # 4. Sélection des colonnes
    TARGET = 'intensity'

    X_train = train[FEATURES_XGBOOST]
    y_train = train[TARGET]
    
    X_test = test[FEATURES_XGBOOST]
    y_test = test[TARGET]
    
    return X_train, y_train, X_test, y_test, test['timestamp']
