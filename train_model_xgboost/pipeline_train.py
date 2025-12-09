# train_model_xgboost/pipeline.py
import logging
import pandas as pd

from train_model_xgboost import (loader, trainer, evaluator, saver)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

def run_xgboost_pipeline():
    logger.info("🚀 DÉMARRAGE DU PIPELINE XGBOOST")

    # 1. Chargement
    df_global = loader.load_full_dataset()
    compteurs = df_global['name'].unique()
    results = []

    # 2. Boucle Compteurs
    for name in compteurs:
        logger.info(f"🔹 XGBoost sur : {name}")
        
        # A. Préparation (X, y)
        X_train, y_train, X_test, y_test, dates_test = loader.get_data_for_counter(df_global, name)
        
        if X_test.empty:
            continue

        # B. Entraînement
        model = trainer.train_model(X_train, y_train)

        # C. Prédiction
        preds = trainer.make_predictions(model, X_test)
        
        # Petit nettoyage : pas de prédictions négatives en vélo !
        preds = [max(0, x) for x in preds]

        # D. Évaluation
        mae, error_pct = evaluator.evaluate(y_test, preds)
        logger.info(f"   📊 Score : MAE={mae} | Err={error_pct}%")

        # E. Sauvegarde
        model_file = saver.save_model(model, name)

        results.append({
            "Compteur": name,
            "MAE": mae,
            "Erreur %": error_pct,
            "Modèle": model_file
        })

    # 3. Bilan
    saver.save_metrics(results)
    
    # Affichage comparatif rapide
    print("\n--- RÉSULTATS XGBOOST ---")
    print(pd.DataFrame(results).sort_values('MAE').to_string(index=False))

if __name__ == "__main__":
    run_xgboost_pipeline()