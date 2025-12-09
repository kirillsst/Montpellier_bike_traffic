import pandas as pd
import joblib
import matplotlib.pyplot as plt
from pathlib import Path

# --- IMPORTS LOCAUX ---
from train_model_xgboost import loader, config

# --- CONFIGURATION ---
TARGET_DATE = "2025-11-24"

def main():
    print(f"--- PRÉDICTION HORAIRE GEOLOCALISÉE : {TARGET_DATE} ---")

    # 1. Chargement & Feature Engineering
    df = loader.load_full_dataset()
    
    target_dt = pd.to_datetime(TARGET_DATE).date()
    df_day = df[df['timestamp'].dt.date == target_dt].copy()
    
    if df_day.empty:
        print(f"⚠️ Aucune donnée pour le {TARGET_DATE}")
        return

    print(f"Données brutes chargées : {len(df_day)} lignes.")
    df_day = loader.create_features(df_day)

    # 2. Préparation du stockage
    # On va stocker une liste de dictionnaires pour créer un DataFrame propre
    predictions_list = []
    
    compteurs = df_day['name'].unique()
    print(f"Chargement des modèles depuis : {config.ARTIFACTS_DIR}")

    # 3. Boucle de Prédiction
    for name in compteurs:
        # A. Isolation des données
        df_c = df_day[df_day['name'] == name].sort_values('timestamp')
        
        # B. Récupération des Coordonnées GPS (Depuis la donnée brute)
        # On prend la première valeur (car la position ne change pas)
        lat = df_c['latitude'].iloc[0]
        lon = df_c['longitude'].iloc[0]

        # C. Chargement Modèle
        safe_name = name.replace(" ", "_").replace("/", "-")
        model_path = config.ARTIFACTS_DIR / f"xgboost_{safe_name}.joblib"
        
        if model_path.exists():
            model = joblib.load(model_path)
            
            # D. Prédiction
            preds = model.predict(df_c[loader.FEATURES_XGBOOST])
            y_pred = [int(max(0, x)) for x in preds] # Nettoyage
            
            # E. Stockage structuré (Format "Long")
            # On parcourt les 24 heures (ou moins si données partielles)
            for i, val in enumerate(y_pred):
                hour = df_c['hour'].iloc[i] if i < len(df_c) else i
                
                predictions_list.append({
                    "name": name,
                    "date": TARGET_DATE,
                    "hour": hour,
                    "predicted_intensity": val,
                    "latitude": lat,
                    "longitude": lon
                })
        else:
            print(f"❌ Modèle manquant pour : {name}")

    # 4. CRÉATION DU DATAFRAME FINAL
    df_final = pd.DataFrame(predictions_list)

    # 5. Export
    out_csv_path = config.BASE_DIR / f"pred_horaire_{TARGET_DATE}.csv"
    df_final.to_csv(out_csv_path, index=False)
    
    print("\n=== APERÇU DU FICHIER GÉNÉRÉ ===")
    print(df_final.head())
    print(f"\n💾 Détail géolocalisé sauvegardé dans : {out_csv_path}")

    # 6. Visualisation Rapide (Total Ville)
    # On pivote juste pour le graphique
    df_pivot = df_final.pivot(index='hour', columns='name', values='predicted_intensity')
    total_ville_pred = df_pivot.sum(axis=1)
    
    real_total = df_day.groupby(df_day['timestamp'].dt.hour)['intensity'].sum()
    real_total = real_total.reindex(range(24), fill_value=0)

    plt.figure(figsize=(12, 6))
    plt.plot(real_total.index, real_total.values, label='Réalité', color='black', linestyle='--')
    plt.plot(total_ville_pred.index, total_ville_pred.values, label='Prédiction', color='#e74c3c', linewidth=3)
    plt.title(f"Prévision {TARGET_DATE} (Données Géolocalisées)", fontsize=14)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.show()

if __name__ == "__main__":
    main()