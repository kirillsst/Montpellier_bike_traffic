import pandas as pd
import joblib
import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# --- IMPORTS LOCAUX ---
# On garde config pour le chemin des modèles, mais on n'utilise plus loader
from train_model_xgboost import config, loader

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Tables
INPUT_TABLE = "counters_forecast"
OUTPUT_TABLE = "predictions_hourly"

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("❌ Erreur : SUPABASE_URL ou SUPABASE_KEY manquant dans le .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def run_prediction_pipeline():
    print(f"🚀 Démarrage de la prédiction depuis '{INPUT_TABLE}'...")
    supabase = get_supabase()

    # 1. CHARGEMENT DES DONNÉES PRÉPARÉES (INFERENCE STORE)
    print("📥 Récupération des données d'entrée...")
    response = supabase.table(INPUT_TABLE).select("*").execute()
    df_day = pd.DataFrame(response.data)

    if df_day.empty:
        print(f"⚠️ La table {INPUT_TABLE} est vide. Lancez d'abord 'prepare_forecast_data.py'.")
        return

    # Conversion et Tri
    df_day['timestamp'] = pd.to_datetime(df_day['timestamp'])
    df_day = df_day.sort_values(by=['name', 'timestamp'])
    
    # On récupère la date cible depuis les données
    target_date = df_day['timestamp'].dt.date.iloc[0]
    target_date_str = str(target_date)
    print(f"📅 Date détectée : {target_date_str}")
    
    # Feature Engineering léger (Juste l'extraction de l'heure et des features temporelles si manquantes)
    # Note : Votre script de préparation a déjà fait le gros du travail (meteo, vacances, etc.)
    df_day['hour'] = df_day['timestamp'].dt.hour
    df_day['dayofyear'] = df_day['timestamp'].dt.dayofyear
    df_day['month'] = df_day['timestamp'].dt.month
    df_day['year'] = df_day['timestamp'].dt.year
    df_day['dayofweek'] = df_day['timestamp'].dt.dayofweek
    
    # 2. PRÉDICTION
    predictions_list = []
    compteurs = df_day['name'].unique()
    print(f"🤖 Chargement des modèles depuis : {config.ARTIFACTS_DIR}")

    for name in compteurs:
        # Isolation du compteur
        df_c = df_day[df_day['name'] == name].copy()
        
        # Récupération Lat/Lon pour la sortie
        lat = df_c['latitude'].iloc[0]
        lon = df_c['longitude'].iloc[0]

        # Chargement du modèle spécifique
        safe_name = name.replace(" ", "_").replace("/", "-")
        model_path = config.ARTIFACTS_DIR / f"xgboost_{safe_name}.joblib"
        
        if model_path.exists():
            model = joblib.load(model_path)
            
            # On s'assure que les colonnes sont dans le bon ordre pour XGBoost
            # loader.FEATURES_XGBOOST contient la liste exacte utilisée à l'entraînement
            try:
                X = df_c[loader.FEATURES_XGBOOST]
                preds = model.predict(X)
                
                # Nettoyage (pas de vélos négatifs)
                y_pred = [int(max(0, x)) for x in preds]
                
                # Stockage des résultats
                for i, val in enumerate(y_pred):
                    hour = df_c['hour'].iloc[i]
                    
                    predictions_list.append({
                        "name": name,
                        "date": target_date_str,
                        "hour": int(hour),
                        "predicted_intensity": val,
                        "latitude": lat,
                        "longitude": lon
                    })
            except KeyError as e:
                print(f"❌ Erreur colonnes pour {name}: {e}")
                print(f"Colonnes dispos : {df_c.columns.tolist()}")
                return
        else:
            print(f"⚠️ Modèle manquant pour : {name}")

    if not predictions_list:
        print("❌ Aucune prédiction générée.")
        return

    # 3. UPLOAD VERS SUPABASE (PREDICTIONS_HOURLY)
    print(f"☁️ Envoi de {len(predictions_list)} prévisions vers '{OUTPUT_TABLE}'...")
    
    # Nettoyage préalable pour cette date (pour éviter les doublons)
    supabase.table(OUTPUT_TABLE).delete().eq("date", target_date_str).execute()
    
    # Batch insert
    batch_size = 100
    for i in range(0, len(predictions_list), batch_size):
        batch = predictions_list[i:i+batch_size]
        try:
            supabase.table(OUTPUT_TABLE).insert(batch).execute()
            print(f"   -> Bloc {i}-{i+len(batch)} ok")
        except Exception as e:
            print(f"❌ Erreur insert : {e}")

    print("✅ Cycle terminé avec succès !")

if __name__ == "__main__":
    run_prediction_pipeline()