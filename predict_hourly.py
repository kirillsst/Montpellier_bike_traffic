import pandas as pd
import joblib
import matplotlib.pyplot as plt
from pathlib import Path

# --- IMPORTS (Depuis la racine) ---
from train_model_xgboost import loader, config

# --- CONFIGURATION ---
TARGET_DATE = "2025-11-30"

def main():
    print(f"--- PRÉDICTION HORAIRE PRODUCTION : {TARGET_DATE} ---")

    # 1. Chargement
    df = loader.load_full_dataset()
    
    # 2. Filtre
    target_dt = pd.to_datetime(TARGET_DATE).date()
    df_day = df[df['timestamp'].dt.date == target_dt].copy()
    
    if df_day.empty:
        print(f"⚠️ Aucune donnée pour le {TARGET_DATE}")
        return

    print(f"Données brutes chargées : {len(df_day)} lignes.")

    # 3. Feature Engineering
    df_day = loader.create_features(df_day)

    # 4. Boucle de Prédiction
    compteurs = df_day['name'].unique()
    results_hourly = {'Heure': list(range(24))}
    
    print(f"Chargement des modèles depuis : {config.ARTIFACTS_DIR}")

    for name in compteurs:
        df_c = df_day[df_day['name'] == name].sort_values('timestamp')
        
        safe_name = name.replace(" ", "_").replace("/", "-")
        model_path = config.ARTIFACTS_DIR / f"xgboost_{safe_name}.joblib"
        
        if model_path.exists():
            model = joblib.load(model_path)
            
            # Prédiction
            preds = model.predict(df_c[loader.FEATURES_XGBOOST])
            
            # Nettoyage
            y_pred = [int(max(0, x)) for x in preds]
            
            if len(y_pred) == 24:
                results_hourly[name] = y_pred
        else:
            print(f"❌ Modèle manquant pour : {name}")

    # 5. Résultats & Visualisation
    df_res = pd.DataFrame(results_hourly)
    df_res.set_index('Heure', inplace=True)
    
    df_res['TOTAL_VILLE'] = df_res.sum(axis=1)

    print("\n=== PRÉDICTIONS PAR HEURE (TOTAL VILLE) ===")
    print(df_res[['TOTAL_VILLE']].T)

    # --- MODIFICATION ICI : SAUVEGARDE À LA RACINE ---
    # config.BASE_DIR pointe déjà vers la racine du projet
    out_csv_path = config.BASE_DIR / f"pred_horaire_{TARGET_DATE}.csv"
    
    df_res.to_csv(out_csv_path)
    print(f"\n💾 Détail sauvegardé à la racine : {out_csv_path}")

    # Graphique
    plt.figure(figsize=(12, 6))
    
    real_total = real_data_total_ville(df_day)
    
    plt.plot(df_res.index, real_total, label='Réalité (Total)', color='black', linewidth=2, linestyle='--')
    plt.plot(df_res.index, df_res['TOTAL_VILLE'], label='Prédiction XGBoost', color='#e74c3c', linewidth=3)
    
    plt.title(f"Prévision Trafic Global - {TARGET_DATE}", fontsize=14)
    plt.xlabel("Heure")
    plt.ylabel("Passages cumulés")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.show()

def real_data_total_ville(df_day):
    grp = df_day.groupby(df_day['timestamp'].dt.hour)['intensity'].sum()
    grp = grp.reindex(range(24), fill_value=0)
    return grp.values

if __name__ == "__main__":
    main()