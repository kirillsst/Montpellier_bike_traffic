# train_model/pipeline.py
import logging

# Import des modules locaux
import loader
import trainer
import evaluator
import saver

# Config Logs
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

def run_training_pipeline():
    logger.info("🚀 DÉMARRAGE DU PIPELINE D'ENTRAÎNEMENT")

    # 1. Chargement global
    df_global = loader.load_full_dataset()
    compteurs = df_global['name'].unique()
    
    results = []

    # 2. Boucle sur chaque compteur
    for name in compteurs:
        logger.info(f"🔹 Traitement : {name}")
        
        # A. Préparation des données
        train_df, test_df = loader.get_data_for_counter(df_global, name)
        
        if test_df.empty:
            logger.warning(f"   ⚠️ Pas de données de test pour {name}. Ignoré.")
            continue

        # B. Entraînement
        model = trainer.train_model(train_df)

        # C. Prédiction (Test)
        forecast = trainer.make_predictions(model, test_df)

        # D. Évaluation
        mae, error_pct = evaluator.evaluate(test_df, forecast)
        logger.info(f"   📊 Score : MAE={mae} | Err={error_pct}%")

        # E. Sauvegarde Modèle
        model_file = saver.save_model(model, name)

        # Stockage des résultats
        results.append({
            "Compteur": name,
            "MAE": mae,
            "Erreur %": error_pct,
            "Fichier": model_file
        })

    # 3. Sauvegarde finale des scores
    saver.save_metrics(results)
    logger.info("🎉 Pipeline terminé avec succès.")

if __name__ == "__main__":
    run_training_pipeline()