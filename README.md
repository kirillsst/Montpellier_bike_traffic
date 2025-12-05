config.py
# train_model_xgboost/config.py
from pathlib import Path

# Chemins
BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_PATH = BASE_DIR / "dataset_final_training.csv"
ARTIFACTS_DIR = BASE_DIR / "train_model_xgboost" / "artifacts"

# Date de séparation Train / Test
CUTOFF_DATE = "2025-11-30" 

ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


loader.py
# train_model_xgboost/loader.py
import pandas as pd
from .config import DATASET_PATH, CUTOFF_DATE

# --- CONSTANTE GLOBALE (Accessible depuis les autres scripts) ---
FEATURES_XGBOOST = [
    'hour', 'dayofweek', 'month', 'year', 'dayofyear', 
    'temperature_2m', 'precipitation', 'precipitation_class', 'windspeed_10m', 'is_raining', 
    'is_vacances', 'is_ferie', 'is_weekend'
]

def load_full_dataset():
    print(f"Chargement XGBoost depuis : {DATASET_PATH}")
    df = pd.read_csv(DATASET_PATH)
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
    return df

def create_features(df):
    """Crée les features temporelles pour XGBoost."""
    df = df.copy()
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
    
    # 4. Sélection des colonnes via la CONSTANTE GLOBALE
    TARGET = 'intensity'

    X_train = train[FEATURES_XGBOOST]
    y_train = train[TARGET]
    
    X_test = test[FEATURES_XGBOOST]
    y_test = test[TARGET]
    
    return X_train, y_train, X_test, y_test, test['timestamp']