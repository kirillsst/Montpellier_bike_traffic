import pandas as pd
from .config import get_supabase_client

# --- CONFIGURATION DU NOM DE COLONNE ---
COL_DATE_METEO = "time"

def get_unique_counters():
    """
    Retourne la liste des 10 compteurs fixes pour la prédiction.
    On les définit en dur pour être sûr d'avoir toujours les 240 lignes.
    """
    print("   [Extract] Chargement de la liste fixe des 10 compteurs...")
    
    counters_list = [
        {"name": "Compteur Vélo Grabels", "latitude": 43.6452, "longitude": 3.8224},
        {"name": "Compteur Vélo Grossec", "latitude": 43.5754, "longitude": 3.8617},
        {"name": "Compteur Vélo Jean Mermoz", "latitude": 43.6115, "longitude": 3.8899},
        {"name": "Compteur Vélo Lattes 1", "latitude": 43.57883, "longitude": 3.93324},
        {"name": "Compteur Vélo Pompignane1", "latitude": 43.614, "longitude": 3.8981},
        {"name": "Compteur Vélo Pompignane2", "latitude": 43.614, "longitude": 3.8981},
        {"name": "Compteur Vélo Renouvier bande cyclable", "latitude": 43.60381, "longitude": 3.8677},
        {"name": "Compteur Vélo Renouvier chaussée", "latitude": 43.60383, "longitude": 3.86779},
        {"name": "Compteur Vélo Vieussens1", "latitude": 43.6001, "longitude": 3.8776},
        {"name": "Compteur Vélo Vieussens2", "latitude": 43.6001, "longitude": 3.8776}
    ]
    
    return pd.DataFrame(counters_list)

def get_meteo_forecast(target_date: str):
    """Récupère la météo pour la date cible."""
    supabase = get_supabase_client()
    print(f"   [Extract] Récupération météo pour le {target_date}...")
    
    resp = supabase.table("meteo_forecast")\
        .select("*")\
        .gte(COL_DATE_METEO, f"{target_date} 00:00:00")\
        .lte(COL_DATE_METEO, f"{target_date} 23:59:59")\
        .execute()
    
    df_meteo = pd.DataFrame(resp.data)
    
    # Gestion du cas "Pas de météo" (Fallback)
    if df_meteo.empty:
        print("   ⚠️ Météo introuvable. Génération de données par défaut.")
        hours = pd.date_range(start=f"{target_date} 00:00:00", end=f"{target_date} 23:00:00", freq='h')
        df_meteo = pd.DataFrame({
            COL_DATE_METEO: hours,
            'temperature_2m': 12.0,
            'precipitation': 0.0,
            'windspeed_10m': 10.0
        })
    
    # Standardisation
    if COL_DATE_METEO in df_meteo.columns:
        df_meteo.rename(columns={COL_DATE_METEO: 'timestamp'}, inplace=True)
        
    if 'timestamp' in df_meteo.columns:
        df_meteo['timestamp'] = pd.to_datetime(df_meteo['timestamp'])
        df_meteo['hour_key'] = df_meteo['timestamp'].dt.hour
        
    return df_meteo

def get_calendar_info(target_date: str):
    """Récupère les infos fériés/vacances."""
    supabase = get_supabase_client()
    print(f"   [Extract] Récupération calendrier pour le {target_date}...")
    
    resp = supabase.table("calendar").select("*").eq("date", target_date).execute()
    
    if resp.data:
        return resp.data[0]
    else:
        print("   ⚠️ Calendrier introuvable. On suppose un jour standard.")
        return {}