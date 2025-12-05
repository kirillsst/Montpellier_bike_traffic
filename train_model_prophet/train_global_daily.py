import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error
from pathlib import Path

# --- CONFIGURATION ---
CUTOFF_DATE = "2025-11-24" 

# Chemin dynamique
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "dataset_final_training.csv"

def train_daily_global():
    print("--- PRÉDICTION DU TRAFIC TOTAL JOURNALIER (Toute la ville) ---")

    # 1. Chargement
    print(f"Chargement depuis : {DATA_PATH}")
    if not DATA_PATH.exists():
        print(f"❌ Erreur : Le fichier {DATA_PATH} n'existe pas.")
        return

    df = pd.read_csv(DATA_PATH)
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
    df['date'] = df['timestamp'].dt.date

    # -------------------------------------------------------------------------
    # 2. AGRÉGATION INTELLIGENTE
    # -------------------------------------------------------------------------
    print("Agrégation des données (10 compteurs -> 1 Ville)...")

    # Étape A : Fusion 10 compteurs -> 1 Ligne par heure
    hourly_global = df.groupby('timestamp').agg({
        'intensity': 'sum',
        'temperature_2m': 'mean',
        'precipitation': 'mean',        
        'precipitation_class': 'max',   
        'is_raining': 'max',            # NOUVEAU : Si 1 capteur dit qu'il pleut, il pleut
        'windspeed_10m': 'mean',
        'is_vacances': 'max',
        'is_ferie': 'max',
        'is_weekend': 'max'
    }).reset_index()

    # Étape B : Fusion 24 heures -> 1 Journée
    hourly_global['date'] = hourly_global['timestamp'].dt.date
    
    df_daily = hourly_global.groupby('date').agg({
        'intensity': 'sum',             # Total vélos
        'temperature_2m': 'mean',       # Temp moyenne
        'precipitation': 'sum',         # Cumul pluie (mm)
        'precipitation_class': 'max',   # Pire météo
        'is_raining': 'max',            # NOUVEAU : Si il a plu 1h, la journée est "pluvieuse"
        'windspeed_10m': 'mean',        # Vent moyen
        'is_vacances': 'max',
        'is_ferie': 'max',
        'is_weekend': 'max'
    }).reset_index()

    # Préparation Prophet
    df_prophet = df_daily.rename(columns={'date': 'ds', 'intensity': 'y'})
    df_prophet['ds'] = pd.to_datetime(df_prophet['ds'])

    print(f"Dataset journalier prêt : {len(df_prophet)} jours.")
    
    # -------------------------------------------------------------------------
    # 3. ENTRAÎNEMENT
    # -------------------------------------------------------------------------
    
    # Split
    train = df_prophet[df_prophet['ds'] < CUTOFF_DATE]
    test = df_prophet[df_prophet['ds'] == CUTOFF_DATE]

    if test.empty:
        print(f"⚠️ Erreur : Pas de données pour la date cible {CUTOFF_DATE}")
        return

    # Configuration Modèle
    m = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        seasonality_mode='multiplicative'
    )

    # Ajout des Régresseurs (AVEC IS_RAINING)
    regressors = [
        'temperature_2m',
        'precipitation',
        'precipitation_class', 
        'is_raining', # <--- AJOUTÉ
        'windspeed_10m',
        'is_vacances',
        'is_ferie'
    ]
    
    for reg in regressors:
        m.add_regressor(reg)

    print("Entraînement en cours...")
    m.fit(train)

    # -------------------------------------------------------------------------
    # 4. PRÉDICTION & RÉSULTATS
    # -------------------------------------------------------------------------
    forecast = m.predict(test)
    
    # Résultats
    reel = test.iloc[0]['y']
    predit = forecast.iloc[0]['yhat']
    ecart = predit - reel
    erreur_pct = (abs(ecart) / reel) * 100

    print("\n" + "="*50)
    print(f"RÉSULTAT DU 30 NOVEMBRE 2025")
    print("="*50)
    print(f"RÉALITÉ    : {int(reel):,} vélos")
    print(f"PRÉDICTION : {int(predit):,} vélos")
    print(f"ÉCART      : {int(ecart):+} vélos")
    print(f"ERREUR     : {erreur_pct:.2f}%")
    print("="*50)

    # -------------------------------------------------------------------------
    # 5. VISUALISATION (Bar Chart)
    # -------------------------------------------------------------------------
    plt.figure(figsize=(8, 6))
    
    bar_colors = ['#2c3e50', '#27ae60'] 
    bars = plt.bar(['Réalité', 'Prédiction'], [reel, predit], color=bar_colors, width=0.5)
    
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                 f'{int(height):,}',
                 ha='center', va='bottom', fontsize=14, fontweight='bold')

    # Titre dynamique
    pluie_bool = test.iloc[0]['is_raining']
    meteo_label = "Pluvieux" if pluie_bool == 1 else "Sec"
    
    plt.title(f"Trafic du 30 Nov 2025\n(Journée : {meteo_label})", fontsize=14)
    plt.ylabel("Nombre total de vélos")
    plt.grid(axis='y', alpha=0.2)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    train_daily_global()