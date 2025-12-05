# train_model/loader.py
import pandas as pd
from config import DATASET_PATH, CUTOFF_DATE

def load_full_dataset():
    """Charge le dataset complet et gère le format de date."""
    print(f"Chargement depuis : {DATASET_PATH}")
    df = pd.read_csv(DATASET_PATH)
    # Prophet requiert des dates sans fuseau horaire
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
    return df

def get_data_for_counter(df, counter_name):
    """Prépare les données pour un compteur spécifique (Format Prophet)."""
    df_c = df[df['name'] == counter_name].copy()
    
    # Renommage standard Prophet
    df_prophet = df_c.rename(columns={'timestamp': 'ds', 'intensity': 'y'})
    
    # Split
    train = df_prophet[df_prophet['ds'] < CUTOFF_DATE]
    test = df_prophet[df_prophet['ds'] >= CUTOFF_DATE]
    
    return train, test